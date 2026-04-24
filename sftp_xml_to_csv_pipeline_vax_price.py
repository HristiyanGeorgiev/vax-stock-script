#!/usr/bin/env python3
"""
SFTP XML → CSV pipeline - Vax Prices
- Connects to SFTP (Paramiko)
- Supports multiple ingestion jobs: each job targets a different file
  pattern within the same SFTP folder (see FEED_JOBS below)
- For each job:
    - Finds source file by wildcard (e.g., price-34-*.xml), picks latest
    - Downloads locally (temp)
    - Streams XML and maps sku → (RRP, sellingPrice)
    - If a sku appears more than once, the last occurrence wins and a
      warning is logged
    - Writes a CSV (sku, RRP, sellingPrice) with a timestamp appended to
      the filename
    - Uploads the CSV to the SAME folder the XML was picked from
    - Moves processed source XML into a TransformedXML subfolder
    - If processing fails, moves the source XML into an Error subfolder so it
      won't be picked up again on the next run
    - Cleans up local temp files automatically
XML format expected:
    <Feed store_id="34" ...>
      <Products>
        <Product>
          <sku><![CDATA[...]]></sku>
          <status><![CDATA[...]]></status>
          <RRP><![CDATA[129.000000]]></RRP>
          <sellingPrice><![CDATA[129.000000]]></sellingPrice>
        </Product>
        ...
      </Products>
    </Feed>
Performance notes:
- Uses iterparse streaming (low memory) and clears processed elements.
- Handles large files without loading entire XML into memory.
Timestamp format: YYYY_MM_DD_HH_MI_SS  →  strftime: "%Y_%m_%d_%H_%M_%S"
Requirements:
    pip3 install paramiko
"""
import os
import fnmatch
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation
import xml.etree.ElementTree as ET
import csv
import paramiko
# -----------------------
# CONFIGURABLE CONSTANTS
# -----------------------
# All credentials MUST be set as environment variables (e.g. GitHub Secrets).
# Nothing sensitive is hardcoded here. The script will exit immediately with a
# clear error if a required variable is missing.
def _require_env(name: str) -> str:
    val = os.getenv(name, "")
    if not val:
        raise SystemExit(f"ERROR: required environment variable '{name}' is not set.")
    return val
# SFTP connection — set these as GitHub Secrets (or .env locally)
SFTP_HOST = _require_env("VAX_SFTP_HOST")
SFTP_PORT = int(os.getenv("VAX_SFTP_PORT", "22"))   # optional, defaults to 22
SFTP_USER = _require_env("VAX_SFTP_USER")
SFTP_PASS = _require_env("VAX_SFTP_PASS")
# -----------------------------------------------------------------------
# FEED JOBS
# All jobs read from the same SFTP folder (SFTP_SRC_DIR).
# Each entry picks a different file pattern within that folder.
#
# Fields per job:
#   src_glob        - filename pattern (wildcards ok, e.g. "price-34-*.xml")
#   result_basename - base name for the output CSV (timestamp will be appended)
# -----------------------------------------------------------------------
SFTP_SRC_DIR = "/Magento/Price"
FEED_JOBS = [
    {"src_glob": "price-34-*.xml", "result_basename": "Price_34"},
    {"src_glob": "price-40-*.xml", "result_basename": "Price_40"},
    {"src_glob": "price-41-*.xml", "result_basename": "Price_41"},
    {"src_glob": "price-42-*.xml", "result_basename": "Price_42"}
]
# Subfolder name within each src_dir where processed XMLs are archived
TRANSFORMED_SUBFOLDER = "TransformedXML"
# Subfolder where XMLs that failed processing are quarantined
ERROR_SUBFOLDER = "Error"


_FORMAT = "%Y_%m_%d_%H_%M_%S"  # YYYY_MM_DD_HH_MI_SS
CSV_HEADER = ["sku", "RRP", "sellingPrice"]
# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
# -----------------------
# SFTP HELPERS
# -----------------------
def _connect():
    """Connect to SFTP using Paramiko with password authentication."""
    logging.info("Connecting to SFTP %s:%s as %s", SFTP_HOST, SFTP_PORT, SFTP_USER)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USER,
        password=SFTP_PASS,
        look_for_keys=False,
        allow_agent=False,
        timeout=60,
    )
    sftp = client.open_sftp()
    logging.info("Connected to SFTP")
    return client, sftp
def _normalize_dir(path: str) -> str:
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    if len(path) > 1:
        path = path.rstrip("/")
    return path
def _ensure_dir(sftp: paramiko.SFTPClient, path: str) -> None:
    """Ensure directory exists on SFTP (recursive mkdir)."""
    path = _normalize_dir(path)
    if path == "/":
        return
    parts = path.split("/")[1:]
    current = "/"
    for p in parts:
        current = current.rstrip("/") + "/" + p
        try:
            sftp.stat(current)
        except IOError:
            sftp.mkdir(current)
def _list_matching(sftp: paramiko.SFTPClient, directory: str, pattern: str):
    """Return list of (name, mtime) for entries in directory matching pattern."""
    directory = _normalize_dir(directory)
    entries = sftp.listdir_attr(directory)
    return [
        (attr.filename, getattr(attr, "st_mtime", 0))
        for attr in entries
        if fnmatch.fnmatch(attr.filename, pattern)
    ]
def _select_source(matches, policy: str):
    if not matches:
        return None
    if policy == "latest":
        return sorted(matches, key=lambda x: (x[1], x[0]))[-1][0]
    return sorted(m[0] for m in matches)[0]
def _download(sftp: paramiko.SFTPClient, remote_dir: str, filename: str, local_path: Path) -> None:
    remote_path = f"{_normalize_dir(remote_dir)}/{filename}"
    logging.info("Downloading %s → %s", remote_path, local_path)
    sftp.get(remote_path, str(local_path))
def _upload(sftp: paramiko.SFTPClient, remote_dir: str, filename: str, local_path: Path) -> None:
    _ensure_dir(sftp, remote_dir)
    remote_path = f"{_normalize_dir(remote_dir)}/{filename}"
    logging.info("Uploading %s → %s", local_path, remote_path)
    sftp.put(str(local_path), remote_path)
def _remote_exists(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    """Return True if a file/dir exists on the SFTP server."""
    try:
        sftp.stat(remote_path)
        return True
    except IOError:
        return False
def _move(sftp: paramiko.SFTPClient, src_dir: str, filename: str, dst_dir: str) -> None:
    """
    Move a remote file from src_dir to dst_dir.
    If a file with the same name already exists at the destination, append a
    timestamp suffix to the moved file so nothing is overwritten.
    """
    _ensure_dir(sftp, dst_dir)
    src = f"{_normalize_dir(src_dir)}/{filename}"
    dst_name = filename
    dst = f"{_normalize_dir(dst_dir)}/{dst_name}"
    if _remote_exists(sftp, dst):
        stem, ext = os.path.splitext(filename)
        suffix = datetime.now().strftime(TIMESTAMP_FORMAT)
        dst_name = f"{stem}_{suffix}{ext}"
        dst = f"{_normalize_dir(dst_dir)}/{dst_name}"
        logging.warning(
            "Destination already exists; archiving as %s to avoid overwrite",
            dst_name,
        )
    logging.info("Moving %s → %s", src, dst)
    sftp.rename(src, dst)
# -----------------------
# XML → CSV (streaming)
# -----------------------
def _parse_price(raw: str):
    """Return a Decimal for a price string, or None if unparseable/empty."""
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None
def parse_and_write_csv(xml_path: Path, csv_path: Path) -> None:
    """
    Streams the XML file and writes a CSV with columns: sku, RRP, sellingPrice.
    - Last occurrence wins if the same sku appears more than once (warned).
    - Rows with empty sku or unparseable prices are skipped (warned).
    - Prices are emitted with two decimal places.
    """
    # Preserve insertion order so the CSV mirrors the XML's natural ordering,
    # overwriting on duplicate sku so last-wins semantics hold.
    rows: dict = {}
    context = ET.iterparse(xml_path, events=("end",))
    for _event, elem in context:
        # Strip namespace prefix if present, then match tag name
        local_tag = elem.tag.rsplit("}", 1)[-1]
        if local_tag == "Product":
            sku = None
            rrp = None
            selling = None
            for child in elem:
                child_tag = child.tag.rsplit("}", 1)[-1]
                if child_tag == "sku":
                    sku = (child.text or "").strip()
                elif child_tag == "RRP":
                    rrp = _parse_price(child.text)
                elif child_tag == "sellingPrice":
                    selling = _parse_price(child.text)
            elem.clear()
            if not sku:
                logging.warning("Skipping <Product> with empty sku")
                continue
            if rrp is None or selling is None:
                logging.warning(
                    "Skipping sku %s — missing/unparseable price (RRP=%r, sellingPrice=%r)",
                    sku, rrp, selling,
                )
                continue
            if sku in rows:
                logging.warning("Duplicate sku %s — later value overwrites earlier", sku)
            rows[sku] = (rrp, selling)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for sku in sorted(rows.keys()):
            rrp, selling = rows[sku]
            writer.writerow([sku, f"{rrp:.2f}", f"{selling:.2f}"])
    logging.info("CSV written: %s (%d rows)", csv_path.name, len(rows))
# -----------------------
# SINGLE JOB RUNNER
# -----------------------
def run_job(sftp: paramiko.SFTPClient, job: dict) -> None:
    src_glob = job["src_glob"]
    result_basename = job.get("result_basename", "Price")
    archive_dir = f"{_normalize_dir(SFTP_SRC_DIR)}/{TRANSFORMED_SUBFOLDER}"
    error_dir = f"{_normalize_dir(SFTP_SRC_DIR)}/{ERROR_SUBFOLDER}"
    logging.info("--- Job: %s/%s ---", SFTP_SRC_DIR, src_glob)
    matches = _list_matching(sftp, SFTP_SRC_DIR, src_glob)
    src_name = _select_source(matches, "latest")
    if not src_name:
        logging.warning("No files match %s/%s — skipping job.", SFTP_SRC_DIR, src_glob)
        return
    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    out_name = f"{result_basename}_{timestamp}.csv"
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        local_xml = tmpdir_path / src_name
        local_csv = tmpdir_path / out_name
        try:
            # 1. Download source XML
            _download(sftp, SFTP_SRC_DIR, src_name, local_xml)
            # 2. Transform XML → CSV
            parse_and_write_csv(local_xml, local_csv)
            # 3. Upload CSV to the same folder the XML came from
            _upload(sftp, SFTP_SRC_DIR, out_name, local_csv)
        except Exception:
            # Processing failed — quarantine the source XML in the Error folder
            # so it isn't re-picked on the next run. Re-raise so the outer
            # handler in main() still logs the failure.
            try:
                _move(sftp, SFTP_SRC_DIR, src_name, error_dir)
                logging.error(
                    "Processing failed for %s — moved to %s",
                    src_name,
                    error_dir,
                )
            except Exception as move_exc:
                logging.error(
                    "Processing failed for %s and moving to Error folder also failed: %s",
                    src_name,
                    move_exc,
                )
            raise
        # 4. Success path: archive the processed XML
        _move(sftp, SFTP_SRC_DIR, src_name, archive_dir)
        # Local cleanup handled by TemporaryDirectory context manager
    logging.info("Job done: %s", src_glob)
# -----------------------
# MAIN PIPELINE
# -----------------------
def main() -> None:
    client, sftp = _connect()
    try:
        for job in FEED_JOBS:
            try:
                run_job(sftp, job)
            except Exception as exc:
                # Log and continue to next job rather than aborting all
                logging.error("Job failed (%s/%s): %s", SFTP_SRC_DIR, job.get("src_glob"), exc)
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass
    logging.info("All jobs complete.")
if __name__ == "__main__":
    main()

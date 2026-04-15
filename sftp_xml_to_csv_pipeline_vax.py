#!/usr/bin/env python3
"""
SFTP XML → CSV pipeline - Vax
- Connects to SFTP (Paramiko)
- Supports multiple ingestion jobs: each job targets a different folder/file
  on the same SFTP server (see FEED_JOBS below)
- For each job:
    - Finds source file by wildcard (e.g., stock-*.xml), picks latest or first
    - Downloads locally (temp)
    - Streams XML and maps sku → qty (sums if sku appears more than once)
    - Writes a CSV (sku, qty) with a timestamp appended to the filename
    - Uploads the CSV to the SAME folder the XML was picked from
    - Moves processed source XML into a TransformedXML subfolder within that folder
    - Cleans up local temp files automatically

XML format expected:
    <Feed ...>
      <Products>
        <Product>
          <sku><![CDATA[...]]></sku>
          <status><![CDATA[...]]></status>
          <qty><![CDATA[...]]></qty>
        </Product>
        ...
      </Products>
    </Feed>

Performance notes:
- Uses iterparse streaming (low memory) and clears processed elements.
- Handles large files without loading entire XML into memory.

Timestamp format: YYYY_MM_DD_MI_SS  →  strftime: "%Y_%m_%d_%M_%S"

Requirements:
    pip3 install paramiko
"""

import os
import fnmatch
import tempfile
import logging
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET
import csv
from collections import defaultdict

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
SFTP_HOST = _require_env("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))   # optional, defaults to 22
SFTP_USER = _require_env("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS", "")          # optional if using key auth
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH", "")            # path to private key (optional)
SFTP_KEY_PASSPHRASE = os.getenv("SFTP_KEY_PASSPHRASE", "") # optional

# -----------------------------------------------------------------------
# FEED JOBS
# Each entry defines one independent ingestion job.
# Add or remove entries to handle more folders/files on the same SFTP.
#
# Fields per job:
#   src_dir       - SFTP folder to look for the source XML
#   src_glob      - filename pattern (wildcards supported, e.g. "stock-*.xml")
#   select_policy - "latest" (by mtime) or "first" (alphabetical)
#   result_basename - base name for the output CSV (timestamp will be appended)
# -----------------------------------------------------------------------
FEED_JOBS = [
    {
        "src_dir": "/feeds/vax/store34",
        "src_glob": "stock-34-*.xml",
        "select_policy": "latest",
        "result_basename": "Stock_34",
    },
    # Add more jobs here, for example:
    # {
    #     "src_dir": "/feeds/vax/store99",
    #     "src_glob": "stock-99-*.xml",
    #     "select_policy": "latest",
    #     "result_basename": "Stock_99",
    # },
]

# Subfolder name within each src_dir where processed XMLs are archived
TRANSFORMED_SUBFOLDER = "TransformedXML"

TIMESTAMP_FORMAT = "%Y_%m_%d_%M_%S"  # YYYY_MM_DD_MI_SS
CSV_HEADER = ["sku", "qty"]

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")


# -----------------------
# SFTP HELPERS
# -----------------------

def _connect():
    """Connect to SFTP using Paramiko; supports password or key auth."""
    logging.info("Connecting to SFTP %s:%s as %s", SFTP_HOST, SFTP_PORT, SFTP_USER)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    pkey = None
    if SFTP_KEY_PATH:
        pkey = paramiko.RSAKey.from_private_key_file(
            SFTP_KEY_PATH, password=SFTP_KEY_PASSPHRASE or None
        )

    client.connect(
        hostname=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USER,
        password=SFTP_PASS or None,
        pkey=pkey,
        look_for_keys=not bool(SFTP_KEY_PATH),
        allow_agent=True,
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


def _move(sftp: paramiko.SFTPClient, src_dir: str, filename: str, dst_dir: str) -> None:
    _ensure_dir(sftp, dst_dir)
    src = f"{_normalize_dir(src_dir)}/{filename}"
    dst = f"{_normalize_dir(dst_dir)}/{filename}"
    logging.info("Moving %s → %s", src, dst)
    sftp.rename(src, dst)


# -----------------------
# XML → CSV (streaming)
# -----------------------

def parse_and_write_csv(xml_path: Path, csv_path: Path) -> None:
    """
    Streams the XML file and writes a CSV with columns: sku, qty.
    - Sums qty for the same sku if it appears more than once.
    - Non-numeric or missing qty values are treated as 0.
    """
    totals: dict = defaultdict(int)

    context = ET.iterparse(xml_path, events=("end",))
    for _event, elem in context:
        # Strip namespace prefix if present, then match tag name
        local_tag = elem.tag.rsplit("}", 1)[-1]
        if local_tag == "Product":
            sku = None
            qty = 0
            for child in elem:
                child_tag = child.tag.rsplit("}", 1)[-1]
                if child_tag == "sku":
                    sku = (child.text or "").strip()
                elif child_tag == "qty":
                    val = (child.text or "").strip()
                    try:
                        qty = int(float(val))
                    except ValueError:
                        qty = 0
            if sku:
                totals[sku] += qty
            elem.clear()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for sku in sorted(totals.keys()):
            writer.writerow([sku, totals[sku]])

    logging.info("CSV written: %s (%d rows)", csv_path.name, len(totals))


# -----------------------
# SINGLE JOB RUNNER
# -----------------------

def run_job(sftp: paramiko.SFTPClient, job: dict) -> None:
    src_dir = job["src_dir"]
    src_glob = job["src_glob"]
    select_policy = job.get("select_policy", "latest")
    result_basename = job.get("result_basename", "Stock")

    archive_dir = f"{_normalize_dir(src_dir)}/{TRANSFORMED_SUBFOLDER}"

    logging.info("--- Job: %s/%s ---", src_dir, src_glob)

    matches = _list_matching(sftp, src_dir, src_glob)
    src_name = _select_source(matches, select_policy)
    if not src_name:
        logging.warning("No files match %s/%s — skipping job.", src_dir, src_glob)
        return

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    out_name = f"{result_basename}_{timestamp}.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        local_xml = tmpdir_path / src_name
        local_csv = tmpdir_path / out_name

        # 1. Download source XML
        _download(sftp, src_dir, src_name, local_xml)

        # 2. Transform XML → CSV
        parse_and_write_csv(local_xml, local_csv)

        # 3. Upload CSV to the same folder the XML came from
        _upload(sftp, src_dir, out_name, local_csv)

        # 4. Move processed XML to TransformedXML subfolder
        _move(sftp, src_dir, src_name, archive_dir)

        # Local cleanup handled by TemporaryDirectory context manager

    logging.info("Job done: %s", src_dir)


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
                logging.error("Job failed (%s/%s): %s", job.get("src_dir"), job.get("src_glob"), exc)
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

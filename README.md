# VAX Stock Script

Connects to an SFTP server, downloads XML stock files, transforms them into CSV format, and uploads the result back to the same folder. The processed XML is then archived. The output CSV is picked up by Hemi to import stock for the customer.

## How it works

For each job defined in `FEED_JOBS`:

1. Connects to the SFTP server
2. Scans `SFTP_SRC_DIR` for files matching `src_glob` (e.g. `stock-34-*.xml`) — picks the latest by modification time
3. Downloads the XML locally to a temp directory
4. Parses the XML and maps `sku → qty` (sums duplicate SKUs)
5. Writes a CSV (`sku, qty`) with a timestamp appended to the filename
6. Uploads the CSV back to `SFTP_SRC_DIR`
7. Moves the processed XML into `SFTP_SRC_DIR/TransformedXML/`
8. Cleans up local temp files

## Expected XML format

```xml
<Feed>
  <Products>
    <Product>
      <sku><![CDATA[ABC123]]></sku>
      <status><![CDATA[In Stock]]></status>
      <qty><![CDATA[10]]></qty>
    </Product>
  </Products>
</Feed>
```

## Configuration

### Environment variables

Set these as GitHub Secrets or in your local `.env`:

| Variable | Required | Description |
|---|---|---|
| `VAX_SFTP_HOST` | Yes | SFTP server hostname |
| `VAX_SFTP_USER` | Yes | SFTP username |
| `VAX_SFTP_PASS` | Yes | SFTP password |
| `VAX_SFTP_PORT` | No | SFTP port (default: `22`) |
| `LOG_LEVEL` | No | Logging level (default: `INFO`) |

### Feed jobs

Edit `SFTP_SRC_DIR` and `FEED_JOBS` in the script to point at the right folder and file patterns:

```python
SFTP_SRC_DIR = "/feeds/vax"

FEED_JOBS = [
    {"src_glob": "stock-34-*.xml", "result_basename": "Stock_34"},
    {"src_glob": "stock-45-*.xml", "result_basename": "Stock_45"},
]
```

- `src_glob` — wildcard pattern to match the source XML filename
- `result_basename` — prefix for the output CSV (a timestamp is appended automatically)

Output CSV filename example: `Stock_34_2025_06_01_30_00.csv`

## Requirements

```
pip install paramiko
```

## Running locally

```bash
export VAX_SFTP_HOST=sftp.example.com
export VAX_SFTP_USER=myuser
export VAX_SFTP_PASS=mypassword

python sftp_xml_to_csv_pipeline_vax.py
```

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


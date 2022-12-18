# Concatenate MF4 files by period (+ optional DBC decoding)

This script lets you process MF4 log files across multiple CANedge devices. The script does the following:

1. List all log files for a list of devices within a specific 'total period'
2. Specify a sub period length (e.g. 24 hours)
3. Identify log files pertaining to each sub period, concatenate them and save the result
4. Optionally, the output file can be DBC decoded before outputting 
5. Saved files are by default named based on the 1st and last timestamp, e.g. `221213-0612-to-221213-1506.mf4`

The data can be fetched from an absolute input path on local disk (e.g. the `LOG/` folder on an SD card) or on S3. The latter requires that you map your S3 input bucket using [TntDrive](https://canlogger.csselectronics.com/canedge-getting-started/transfer-data/server-tools/other-s3-tools/). The output files can be stored on your local disk or e.g. on another S3 bucket (also mapped via TntDrive).

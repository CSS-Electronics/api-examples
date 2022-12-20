# Concatenate MF4 files by period (+ optional DBC decoding)

This script lets you process MF4 log files across multiple CANedge devices. The script does the following:

1. List all log files for a list of devices within a specific 'total period'
2. Specify a sub period length (e.g. 24 hours)
3. Identify log files pertaining to each sub period, concatenate them and save the result
4. Optionally, the output file can be DBC decoded before it is saved 
5. Saved files are named based on the 1st and last timestamp, e.g. `221213-0612-to-221213-1506.mf4`

The data can be fetched from an absolute input path on local disk (e.g. the `LOG/` folder on an SD card) and saved locally. 

Alternatively, the files can be loaded/saved directly from/to S3 buckets. This requires that you map your S3 input/output bucket(s) using [TntDrive](https://canlogger.csselectronics.com/canedge-getting-started/transfer-data/server-tools/other-s3-tools/). 


## Installation

See the README in the root of the api-examples repository. 

## Regarding file structure 

Note that the script relies on the `canedge_browser` module to list log files. In order for this to work, your log files must be structured correctly, i.e. `<path_input>/<DEVICEID>/<SESSION>/<SPLIT>`.

## Processing compressed (*.MFC) files 

The script can also be used to process compressed files. To do so, set `finalize_log_files = True`. In this case, the script uses the `mdf2finalized.exe` MF4 converter to convert the MFC files to MF4 and output these into a temporary folder. The script then loads the MF4 files, processes them and deletes the temporary folder for each loop.

## Dynamic script automation

The script can be easily modified to run in a dynamic/automated way. For example, you can update the `period_start` and `period_stop` as below and setup a daily task (e.g. via Windows Task Scheduler) to execute the script via a `.bat` file. 

```
period_start = datetime.now(timezone.utc) - timedelta(days=1)
period_stop = datetime(year=2030, month=1, day=1, hour=1, tzinfo=timezone.utc)
```
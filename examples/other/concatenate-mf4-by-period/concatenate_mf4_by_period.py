"""
About: List MF4 log files by period using the CANedge Python API
and concatenate them into 'combined' MF4 files using the asammdf Python API.
Optionally use TntDrive to map S3 server as local drive to work with S3 directly:
https://canlogger.csselectronics.com/canedge-getting-started/transfer-data/server-tools/other-s3-tools/
"""
import canedge_browser
from asammdf import MDF
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concatenate_utils import extract_mdf_start_stop_time, hour_rounder
import sys


# specify input path for MF4 files (e.g. "D:/LOG" for SD, "Z:" for mapped S3 bucket, ...)
input_root = Path("Z:")

# specify output path (e.g. other mapped S3 bucket, local disk, ...)
output_root = Path("C:/concatenate-mf4-by-period")

# specify which period you wish to process and the max period length of each concatenated log file
period_start = datetime(year=2022, month=12, day=12, hour=2, tzinfo=timezone.utc)
period_stop = datetime(year=2022, month=12, day=16, hour=2, tzinfo=timezone.utc)
file_length_hours = 24

# specify devices to process (from the input_root folder)
devices = ["2F6913DB", "00000000"]

# optionally DBC decode the data
dbc_path = input_root / "dbc_files"
dbc_files = {"CAN": [(dbc, 0) for dbc in list(dbc_path.glob("*" + ".DBC"))]}
enable_dbc_decoding = False

# ----------------------------------------
fs = canedge_browser.LocalFileSystem(base_path=input_root)

for device in devices:
    cnt_input_files = 0
    cnt_output_files = 0
    cnt_sub_period = 0
    sub_period_start = period_start
    sub_period_stop = period_start
    files_to_skip = []

    log_files_total = canedge_browser.get_log_files(fs, device, start_date=period_start,stop_date=period_stop)
    log_files_total = [Path(input_root,log_file) for log_file in log_files_total]
    print(f"\n-----------\nProcessing device {device} | sub period length: {file_length_hours} hours | start: {period_start} | stop: {period_stop} | {len(log_files_total)} log file(s): ",log_files_total)

    # check whether to update sub_period_start to equal 2nd log file start for efficiency
    mdf = MDF(log_files_total[0])
    mdf_start, mdf_stop = extract_mdf_start_stop_time(mdf)

    if mdf_stop < sub_period_start:
        print("First log file is before period start (skip): ", log_files_total[0])
        files_to_skip.append(log_files_total[0])
        if len(log_files_total) == 1:
            continue
        elif len(log_files_total) > 1:
            mdf = MDF(log_files_total[1])
            mdf_start, mdf_stop = extract_mdf_start_stop_time(mdf)
            sub_period_start = hour_rounder(mdf_start)
            print(f"Period start updated to {sub_period_start}")

    # process each sub period for the device
    while sub_period_stop <= period_stop:
        cnt_sub_period += 1
        sub_period_stop = sub_period_start + timedelta(hours=file_length_hours)

        # list log files for the sub period
        log_files = canedge_browser.get_log_files(fs, device, start_date=sub_period_start,stop_date=sub_period_stop)
        log_files = [Path(input_root,log_file) for log_file in log_files]
        log_files = [log_file for log_file in log_files if log_file not in files_to_skip]
        if len(log_files) > 0:
            print(f"\n- Sub period #{cnt_sub_period} \t\t\t| start: {sub_period_start} | stop: {sub_period_stop} | {len(log_files)} log file(s): ", log_files)

        if len(log_files) == 0:
            sub_period_start = sub_period_stop
            continue

        # concatenate all sub period files and identify the delta sec to start/stop
        mdf = MDF.concatenate(log_files)
        mdf_start, mdf_stop = extract_mdf_start_stop_time(mdf)
        mdf_header_start = mdf.header.start_time
        start_delta = (sub_period_start - mdf_header_start).total_seconds()
        stop_delta = (sub_period_stop - mdf_header_start).total_seconds()
        print(f"- Concatenated MF4 created (pre cut)\t| start: {mdf_start} | stop: {mdf_stop}")

        # cut the log file to only include intended period
        mdf = mdf.cut(start=start_delta, stop=stop_delta, whence=0,include_ends=False, time_from_zero=False)
        mdf_start, mdf_stop = extract_mdf_start_stop_time(mdf)

        # convert the start/stop time to string format for file-saving
        mdf_start_str = mdf_start.strftime(f"%y%m%d-%H%M")
        mdf_stop_str = mdf_stop.strftime(f"%y%m%d-%H%M")
        output_file_name = f"{device}/{mdf_start_str}-to-{mdf_stop_str}.MF4"
        output_path = output_root / output_file_name

        # DBC decode the data before saving
        if enable_dbc_decoding:
            mdf = mdf.extract_bus_logging(dbc_files)

        # save the cut MF4 to local disk
        mdf.save(output_path, overwrite=True)
        print(f"- Concatenated MF4 saved (cut)\t\t| start: {mdf_start} | stop: {mdf_stop} | {output_path}")

        cnt_output_files += 1
        sub_period_start = sub_period_stop

        # check if the last log file is fully within sub period (i.e. skip it during next cycle)
        if mdf_stop < sub_period_stop:
            files_to_skip.append(log_files[-1])

            if log_files[-1] == log_files_total[-1]:
                print(f"- Completed processing device {device}")
                break

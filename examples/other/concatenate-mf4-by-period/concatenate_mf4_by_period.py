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
import sys,os, shutil
import subprocess, glob
import gc

path_script = Path(__file__).parent.absolute()

# specify input paths for MF4 files (e.g. on Windows Path("D:\\LOG") for SD, Path("Z:\\") for mapped S3 bucket,
# path_script / "LOG" for relative folder, C:\\Users\\myuser\\folder\\subfolder\\LOG for absolute path, ...)
path_input = path_script / "LOG"

# specify devices to process from path_input
devices = ["2F6913DB"]

# specify output path (e.g. another mapped S3 bucket, local disk, ...)
path_output = path_script / "mf4-output/concatenated"
path_output_temp = path_script / "mf4-output/temp"

# optionally finalize files (if *.MFC) and DBC decode them
finalize_log_files = True
enable_dbc_decoding = False
path_dbc_files = path_script / "dbc_files"
path_mdf2finalized = path_script  / "mdf2finalized.exe"

# specify which period you wish to process and the max period length of each concatenated log file
period_start = datetime(year=2023, month=1, day=1, hour=2, tzinfo=timezone.utc)
period_stop = datetime(year=2023, month=12, day=31, hour=2, tzinfo=timezone.utc)
file_length_hours = 24

# ----------------------------------------
fs = canedge_browser.LocalFileSystem(base_path=path_input)

print("path_input: ",path_input)
dbc_files = {"CAN": [(dbc, 0) for dbc in list(path_dbc_files.glob("*" + ".DBC"))]}

for device in devices:
    cnt_sub_period = 0
    sub_period_start = period_start
    sub_period_stop = period_start
    files_to_skip = []

    log_files_total = canedge_browser.get_log_files(fs, device, start_date=period_start,stop_date=period_stop)
    log_files_total = [path_input.joinpath(log_file[1:]) for log_file in log_files_total]
    
    print(f"\n-----------\nProcessing device {device} | sub period length: {file_length_hours} hours | start: {period_start} | stop: {period_stop} \n{len(log_files_total)} log file(s): ",log_files_total)

    # check whether to update sub_period_start to equal 2nd log file start for efficiency
    if len(log_files_total) == 0:
        print("Skipping device")
        continue
    
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
        log_files_orig_path = canedge_browser.get_log_files(fs, device, start_date=sub_period_start,stop_date=sub_period_stop)
        log_files_orig_path = [path_input.joinpath(log_file[1:]) for log_file in log_files_orig_path]
        log_files = [log_file for log_file in log_files_orig_path if log_file not in files_to_skip]

        if len(log_files) > 0:
            print(f"\n- Sub period #{cnt_sub_period} \t\t\t| start: {sub_period_start} | stop: {sub_period_stop} \n- {len(log_files)} log file(s): ", log_files)

        if len(log_files) == 0:
            sub_period_start = sub_period_stop
            continue

        # finalize MF4 files and output to temporary folder
        if finalize_log_files:
            for log_file in log_files:
                path_output_file_temp_name = Path(*log_file.parts[1:3])
                subprocess.run([path_mdf2finalized, "-i", log_file, "-O", path_output_temp / path_output_file_temp_name,])
            log_files = list(path_output_temp.glob('**/*.MF4'))

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
        path_output_file = path_output / output_file_name

        # DBC decode the data before saving
        if enable_dbc_decoding:
            mdf = mdf.extract_bus_logging(dbc_files)

        # save the cut MF4 to local disk
        mdf.save(path_output_file, overwrite=True)
        print(f"- Concatenated MF4 saved (cut)\t\t| start: {mdf_start} | stop: {mdf_stop} \n- Output path: {path_output_file}")

        # clear MDF
        mdf = mdf.close()
        del mdf
        gc.collect()

        # if temp folder is used, clear it
        if finalize_log_files and os.path.exists(path_output_temp):
            print("- Deleting temporary folder")
            shutil.rmtree(path_output_temp)

        # check if the last log file is fully within sub period (i.e. skip it during next cycle)
        if mdf_stop < sub_period_stop:
            files_to_skip.append(log_files_orig_path[-1])

            if log_files_orig_path[-1] == log_files_total[-1]:
                print(f"- Completed processing device {device}")
                break

        # update sub period start
        sub_period_start = sub_period_stop
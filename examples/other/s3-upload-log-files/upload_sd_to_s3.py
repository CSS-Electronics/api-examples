# This script can be used for 'manually' uploading CANedge log files from an SD to S3.
# The script includes S3 meta data such as firmware and SD timestamp 
# Tested with FW 01.09.XX
# To use the script, add your MF4/MFC files in a LOG/ folder and place your config-01.09.json next to the script


import json
import sys
import mdf_iter
from pathlib import Path
import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig


# specify devices to process from local disk
devices = ["LOG"]
session_offset = 0  # optionally offset the session counter for the uploaded files

# load S3 config from JSON file (expected next to this script)
config_path = Path(__file__).parent / (sys.argv[1] if len(sys.argv) > 1 else "config-01.09.json")
with open(config_path, "r") as f:
    config = json.load(f)

s3_cfg = config["connect"]["s3"]["server"]
key = s3_cfg["accesskey"]
secret = s3_cfg["secretkey"]
endpoint = s3_cfg["endpoint"]
bucket = s3_cfg["bucket"]


# ----------------------------------
# load all log files from local folder
base_path = Path(__file__).parent
log_files = []
for device in devices:
    device_path = base_path / device
    if device_path.exists():
        for ext in ("*.MF4", "*.MFC"):
            log_files += sorted("/" + str(f.relative_to(base_path)).replace("\\", "/") for f in device_path.rglob(ext))
log_files = sorted(set(log_files))
print(f"Found a total of {len(log_files)} log files")

s3 = boto3.client(
    "s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret, config=Config(signature_version="s3v4"),
)

transfer_config = TransferConfig(multipart_threshold=9999999999999999, max_concurrency=10, num_download_attempts=10)

put_index = 0

# for each log file, extract header information, create S3 key and upload
for log_file in log_files:

    with open(base_path / log_file[1:], "rb") as handle:
        mdf_file = mdf_iter.MdfFile(handle)
        header = "HDcomment.Device Information"

        device_id = mdf_file.get_metadata()[f"{header}.serial number"]["value_raw"]
        session = mdf_file.get_metadata()[f"HDcomment.File Information.session"]["value_raw"]
        session = f"{(int(session) + session_offset):08}"
        split = int(mdf_file.get_metadata()[f"HDcomment.File Information.split"]["value_raw"])
        split = f"{split:08}"
        ext = log_file.split(".")[-1]

        s3_meta_hw = mdf_file.get_metadata()[f"{header}.hardware version"]["value_raw"] + "/00.00"
        s3_meta_fw = mdf_file.get_metadata()[f"{header}.firmware version"]["value_raw"]
        s3_meta_timestamp = mdf_file.get_data_frame().index.min().strftime("%Y%m%dT%H%M%SZ")

    s3_key = f"{device_id}/{session}/{split}.{ext}"
    put_index += 1
    s3_meta = {
        "Metadata": {
            "Hw": s3_meta_hw,
            "Fw": s3_meta_fw,
            "Net": "sd_manual",
            "Timestamp": s3_meta_timestamp,
            "Put-Index": str(put_index),
        }
    }

    # upload local file to S3
    s3.upload_file(str(base_path / log_file[1:]), bucket, s3_key, ExtraArgs=s3_meta, Config=transfer_config)
    print(f"Uploaded {log_file} as {s3_key}")

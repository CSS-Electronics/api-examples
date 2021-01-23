# This script can be used for 'manually' uploading CANedge log files from an SD to S3.
# The script includes S3 meta data such as firmware and SD timestamp and correctly derives S3 key.

import mdf_iter
import canedge_browser
from pathlib import Path
import boto3
from botocore.client import Config
from s3transfer import TransferConfig, S3Transfer


# specify devices to process from local disk
devices = ["LOG/958D2219"]
session_offset = 0  # optionally offset the session counter for the uploaded files

# specify target S3 bucket details
key = "s3_key"
secret = "s3_secret"
endpoint = "s3_endpoint"  # e.g. https://s3.eu-central-1.amazonaws.com
bucket = "s3_bucket"


# ----------------------------------
# load all log files from local folder
base_path = Path(__file__).parent
fs = canedge_browser.LocalFileSystem(base_path=base_path)
log_files = canedge_browser.get_log_files(fs, devices)
print(f"Found a total of {len(log_files)} log files")

s3 = boto3.client(
    "s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret, config=Config(signature_version="s3v4"),
)

transfer = S3Transfer(s3, TransferConfig(multipart_threshold=9999999999999999, max_concurrency=10, num_download_attempts=10,))

# for each log file, extract header information, create S3 key and upload
for log_file in log_files:

    with fs.open(log_file, "rb") as handle:
        mdf_file = mdf_iter.MdfFile(handle)
        header = "HDComment.Device Information"

        device_id = mdf_file.get_metadata()[f"{header}.serial number"]["value_raw"]
        session = mdf_file.get_metadata()[f"{header}.File Information.session"]["value_raw"]
        session = f"{(int(session) + session_offset):08}"
        split = int(mdf_file.get_metadata()[f"{header}.File Information.split"]["value_raw"])
        split = f"{split:08}"
        ext = log_file.split(".")[-1]

        s3_meta_fw = mdf_file.get_metadata()[f"{header}.firmware version"]["value_raw"]
        s3_meta_timestamp = mdf_file.get_data_frame().index.min().strftime("%Y%m%dT%H%M%S")

    s3_key = f"{device_id}/{session}/{split}.{ext}"
    s3_meta = {"Metadata": {"Fw": s3_meta_fw, "Timestamp": s3_meta_timestamp}}

    # upload local file to S3
    transfer.upload_file(log_file[1:], key=s3_key, bucket=bucket, extra_args=s3_meta)
    print(f"Uploaded {log_file} as {s3_key}")

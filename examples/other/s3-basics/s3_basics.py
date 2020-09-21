"""
About: Showcase basic S3 server operations (download, upload, delete, ...).
Note that the S3 API (called boto3 in Python) can be used across S3 servers, incl. AWS and MinIO.
Note: If you need to use MinIO with a self-signed certificate (TLS), we suggest using the MinIO Python SDK.
Test: Tested on MinIO S3 and AWS S3 - you can test with your own server
"""

import boto3, re
from botocore.client import Config
from datetime import datetime
from s3_get_keys import get_keys

# initialize S3 resource
endpoint = "http://127.0.0.1:9000"  # e.g. "https://s3.amazonaws.com"  for us-east-1 AWS S3 server
access_key = "CANedgeTestServerAccessKey"
secret_key = "MySecretPassword"
bucket_name = "ce2-source"
region_name = "us-east-1"

s3 = boto3.resource(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name=region_name,
)
bucket = s3.Bucket(bucket_name)


# create a selective list of S3 object keys using get_keys (see s3_get_keys.py)
keys = []
for key in get_keys(
    s3=s3,
    bucket_name=bucket_name,
    prefix="",
    suffix=".mf4",
    date_start=datetime(2020, 1, 1, 19, 54, 0),
    date_end=datetime(2020, 10, 10, 19, 56, 0),
):
    keys.append(key)

print("\nObject keys: ", keys)


# list all device serial numbers in a bucket
devices = []
result = bucket.meta.client.list_objects_v2(Bucket=bucket_name, Delimiter="/")
for obj in result.get("CommonPrefixes"):
    if re.compile("^[0-9a-fA-F]{8}/").match(obj["Prefix"]):
        devices.append(obj["Prefix"].split("/")[0])

print(f"\nDevices in bucket {bucket_name}: ", devices)


# download object from S3 (specify a device connected to your S3 server)
device = "31CB1F25"
s3_key = device + "/device.json"
local_path = s3_key.replace("/", "_")

try:
    bucket.download_file(s3_key, local_path)
    print(f"Downloaded S3 object {s3_key} to local path {local_path}")
except:
    print(f"Warning: Unable to download {s3_key}")

# get device S3 meta data object
try:
    meta = s3.meta.client.head_object(Bucket=bucket_name, Key=s3_key,)[
        "ResponseMetadata"
    ]["HTTPHeaders"]
    print(f"S3 meta data of object {s3_key}:", meta)
except:
    print(f"Warning: Unable to get meta data of {s3_key}")


# upload file to S3
try:
    s3_key_upload = s3_key.replace(".json", "-upload.json")
    bucket.upload_file(local_path, Key=s3_key_upload)
    print(f"Uploaded {local_path} as {s3_key_upload}")
except:
    print(f"Warning: Unable to upload {local_path} as {s3_key_upload}")

# delete object from S3
try:
    bucket.Object(s3_key_upload).delete()
    print(f"Deleted {s3_key_upload} from S3")
except:
    print(f"Warning: Unable to delete {s3_key_upload} from S3")

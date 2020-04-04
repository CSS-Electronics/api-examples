"""
About: Showcase basic S3 server operations (download, upload, delete, ...).
Note that the S3 API (called boto3 in Python) can be used across S3 servers, incl. AWS and MinIO.
Test: You can test the below with your own S3 server
"""

import boto3, re
from botocore.client import Config

# initialize S3 resource
end_point = "https://s3.amazonaws.com"  # Minio: E.g. 'http://192.168.0.14:9000'
access_key = "INSERT_YOUR_ACCESS_KEY"
secret_access_key = "INSERT_YOUR_SECRET_KEY"
bucket_name = "insert_your_bucket_name"
region_name = "us-east-1"

s3 = boto3.resource(
    "s3",
    endpoint_url=end_point,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    config=Config(signature_version="s3v4"),
    region_name=region_name,
)
bucket = s3.Bucket(bucket_name)

# download object from S3
device = "CBC40C04"
s3_path = device + "/device.json"
local_path = s3_path.replace("/", "_")
bucket.download_file(s3_path, local_path)

# upload file to S3
s3_path_upload = s3_path.replace(".json", "_upload.json")
bucket.upload_file(local_path, Key=s3_path_upload)

# delete object from S3
bucket.Object(s3_path_upload).delete()

# list all device serial numbers in a bucket
devices = []
result = bucket.meta.client.list_objects_v2(Bucket=bucket_name, Delimiter="/")
for obj in result.get("CommonPrefixes"):
    if re.compile("^[0-9a-fA-F]{8}/").match(obj["Prefix"]):
        devices.append(obj["Prefix"].split("/")[0])

print(f"Devices in bucket {bucket_name}: ", devices)

"""
About: This example shows how you can access your S3 server as if it was a local file system via s3fs.
You can use s3fs on Linux to mount S3 as a local drive - and in Python to use e.g. cp, mv, ls, du, glob, ...
as if you were dealing with local files (see CANedge2 Intro). s3fs can also be used to migrate local data processing
code to S3. With dask, you can also load multiple CSV files into a single dataframe - useful for big data processing.
For details, see s3fs docs (https://s3fs.readthedocs.io/en/latest/) and dask docs(https://dask.org/)
Test: Tested on MinIO S3 and AWS S3 - you can test with your own server
"""

import s3fs, glob, json, sys
import pandas as pd
import dask.dataframe as dd
from asammdf import MDF


# initialize S3 resource for s3fs
endpoint = "http://127.0.0.1:9000"  # e.g. "https://s3.amazonaws.com"  for us-east-1 AWS S3 server
access_key = "CANedgeTestServerAccessKey"
secret_key = "MySecretPassword"
bucket_name = "canedge-test-bucket"
region_name = "us-east-1"


fs = s3fs.S3FileSystem(
    anon=False,
    client_kwargs={
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    },
)

# list prefixes in bucket
print(fs.ls(bucket_name))


# open a JSON file and parse the contents
device_folder = bucket_name + "/1B29E974"
s3_key = device_folder + "/device.json"
with fs.open(s3_key, "rb") as f:
    device_json = f.read().decode("utf8").replace("'", '"')
    device_json = json.loads(device_json)
    print(device_json)


# load an MDF file via asammdf
s3_key = device_folder + "/00000001/00000001.mf4"
with fs.open(s3_key, "rb") as f:
    mdf = MDF(f)
    df = mdf.to_dataframe(time_as_date=True)
    print(df)


# open a CSV file (e.g. from the mdf2csv event converter) as a pandas dataframe
s3_key = device_folder + "/00000001/00000002_CAN.csv"
with fs.open(s3_key, "rb") as f:
    df = pd.read_csv(f, sep=";")
    print(df)

# open multiple CSV files via dask (note the slight syntax differences)
s3_folder = "s3://" + bucket_name + "/" + device + "/00000001/"
s3_keys = [s3_folder + "00000002_CAN.csv", s3_folder + "00000003_CAN.csv"]

df = dd.read_csv(
    s3_keys,
    sep=";",
    storage_options={
        "client_kwargs": {"endpoint_url": endpoint},
        "key": access_key,
        "secret": secret_key,
    },
)

print(df.head())
rows = df["TimestampEpoch"].count().compute()
print(f"Total rows in dask dataframe: {rows}")

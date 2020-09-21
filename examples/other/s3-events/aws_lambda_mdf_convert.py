"""
About: This is a basic AWS Lambda handler function for event-based processing of uploaded log files - see README for details
Test: Last tested on April 4, 2020 with MDF4 sample data
"""
from __future__ import print_function
import boto3
import subprocess
import glob
import re

s3 = boto3.client("s3")


def lambda_handler(event, context):
    # specify the target bucket for the output and the converter name
    target_bucket = "ce2-lambda-target"
    converter_name = "mdf2asc"

    # load converter and list relevant support_files
    converter = glob.glob(converter_name)[0]
    support_files = ["passwords.json"]

    # extract source_bucket and key from event
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    print(f"Event: {key} uploaded to {source_bucket}")

    # download the object to tmp folder
    local_key = "/tmp/" + key.replace("/", "%2F")
    print(f"Set local_key: {local_key}")

    s3.download_file(source_bucket, key, local_key)
    print(f"Downloaded object as {local_key}")

    # move support files and the MDF4 converter to tmp and process the object
    for file in support_files:
        subprocess.run(["cp", "./" + file, "/tmp"])

    subprocess.run(["cp", "./" + converter, "/tmp"])
    subprocess.run(["chmod", "+x", "/tmp/" + converter])
    subprocess.run(["/tmp/" + converter, "-i", local_key])

    # select the converted objects
    objects_all = glob.glob("/tmp/*")
    objects_conv = [
        obj
        for obj in objects_all
        if not re.search(f"(passwords.json$|{converter}$|{local_key}$)", obj)
    ]

    print("All objects in tmp/:", objects_all)
    print("Converted objects:", objects_conv)

    # upload the converted objects to target destination
    for obj in objects_conv:
        # (optionally add e.g. analytics based conditioning here)
        target_key = obj.replace("/tmp/", "").replace("%2F", "/")
        s3.upload_file(obj, target_bucket, target_key)
        print(f"Uploaded {obj} to {target_bucket} as {target_key}")

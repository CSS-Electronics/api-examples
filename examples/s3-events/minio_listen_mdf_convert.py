"""
About: This is a basic MinIO 'listen bucket notification' function for event-based processing of uploaded log files - see README for details
Note that this example uses the MinIO S3 SDK as listen_bucket_notification is specific to MinIO - but the AWS S3 SDK could have been used for the rest
Test: Last tested on April 4, 2020 with MDF4 sample data
"""

from minio import Minio
import glob, subprocess, re, tempfile, os

# variables
prefix = ""  # use to optionally specify a specific device
suffix = ".MF4"  # set to match the file extension your devices are uploading with (.MF4, .MFE, .MFM)

endpoint = "127.0.0.1:9000"
access_key = "CANedgeTestServerAccessKey"
secret_key = "MySecretPassword"
source_bucket = "ce2-source"
secure = False

# if using TLS (HTTPS), set secure = True and set the path below to your public.crt certificate:
# os.environ["SSL_CERT_FILE"] = "C:\\Users\\marti\\.minio\\certs\\public.crt"

target_bucket = "ce2-target"
converter = "mdf2csv.exe"

client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

# listen to events
events = client.listen_bucket_notification(
    source_bucket, prefix, suffix, ["s3:ObjectCreated:*"]
)

print("Initialized - awaiting events ... [CTRL + C to exit]\n")

for event in events:
    # extract source_bucket and key from event
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"].replace("%2F", "/")
    print(f"\n\nEvent: {key} uploaded to {source_bucket}")

    # download the object to tmp folder
    f = tempfile.TemporaryDirectory()
    tmp = f.name + "\\"
    local_key = tmp + key.replace("/", "%2F")
    print(f"Set local_key: {local_key}")

    client.fget_object(source_bucket, key, local_key)
    print(f"Downloaded object as {local_key}")

    # convert the object
    subprocess.run([converter, "-i", local_key])

    # select the converted objects
    objects_all = glob.glob(tmp + "*")
    objects_conv = [
        obj
        for obj in objects_all
        if not re.search(f"(passwords.json$|.exe$|{suffix}$)", obj)
    ]
    print("Objects in temporary directory:\n", objects_all)
    print("Successfully converted objects:\n", objects_conv)

    # upload the converted objects to target destination
    for obj in objects_conv:
        # (optionally add e.g. analytics based conditioning here)
        target_key = obj.replace(tmp, "").replace("%2F", "/")
        client.fput_object(target_bucket, target_key, obj)
        print(f"Uploaded {obj} to {target_bucket} as {target_key}")

    f.cleanup()

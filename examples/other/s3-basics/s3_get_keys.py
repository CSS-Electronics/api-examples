"""
About: This function can be used to extract specific S3 keys, utilzing the CANedge S3 meta data timestamp as
optional segmentation. Note that this timestamp reflects the time a file was created on the device SD card -
not the time it was uploaded. You can ignore the timestamp by leaving the start/end blank.
You can fetch data from a specific device by adding a prefix - or specific file types by adding a suffix.
"""
from datetime import datetime


def get_keys(
    s3, bucket_name, prefix="", suffix="", date_start=datetime(1900, 1, 1, 0, 0, 0), date_end=datetime(2100, 1, 1, 0, 0, 0),
):
    kwargs = {"Bucket": bucket_name, "Prefix": prefix}
    while True:
        resp = s3.meta.client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            key = obj["Key"]
            meta = s3.meta.client.head_object(Bucket=bucket_name, Key=key)
            date_time = datetime(1900, 1, 1, 0, 0, 0)
            if key.endswith(suffix) and datetime(1900, 1, 1, 0, 0, 0) < date_start and date_end < datetime(2100, 1, 1, 0, 0, 0):
                try:
                    date_time = datetime.strptime(
                        str(meta["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-timestamp"]), "%Y%m%dT%H%M%S",
                    )
                except:
                    print("Object " + key + " was excluded (no valid meta timestamp)")
                    date_time = datetime(1800, 1, 1, 0, 0, 0)
            if key.endswith(suffix) and date_start <= date_time and date_time <= date_end:
                yield key
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

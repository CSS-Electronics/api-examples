import mdf_iter
import canedge_browser
import can_decoder

from datetime import datetime, timezone


def setup_fs_s3():
    """Helper function to setup a remote S3 filesystem connection.
    """
    import s3fs

    fs = s3fs.S3FileSystem(
        key="<key>", secret="<secret>", client_kwargs={"endpoint_url": "<endpoint>"},
    )

    return fs


def setup_fs():
    """Helper function to setup the local file system.
    """
    from fsspec.implementations.local import LocalFileSystem
    from pathlib import Path

    fs = LocalFileSystem()

    # Setup path to local folder structure, as if copied from a CANedge SD.
    # Assumes the folder is placed in same directory as this file and named "LOG".
    base_path = Path(__file__).parent

    return fs, str(base_path)


# specify which devices to process (from local folder or S3 bucket)
devices = ["LOG/958D2219"]

# specify which time period to fetch log files for
start = datetime(year=2020, month=1, day=1, hour=1, minute=1, tzinfo=timezone.utc)
stop = datetime(year=2099, month=1, day=1, tzinfo=timezone.utc)

# specify DBC path
dbc_path = r"CSS-Electronics-SAE-J1939-DEMO.dbc"

# ---------------------------------------------------
# initialize DBC converter and file loader
db = can_decoder.load_dbc(dbc_path)
df_decoder = can_decoder.DataFrameDecoder(db)

# fs = setup_fs_s3()
fs, folder_name = setup_fs()

# List log files from your S3 server
log_files = canedge_browser.get_log_files(
    fs, devices, folder_name, start_date=start, stop_date=stop
)
print(f"Found a total of {len(log_files)} log files")

for log_file in log_files:
    # open log file, get device id and extract dataframe with raw CAN data
    print(f"\nProcessing log file: {log_file}")
    with fs.open(log_file, "rb") as handle:
        mdf_file = mdf_iter.MdfFile(handle)
        device_id = mdf_file.get_metadata()[
            "HDComment.Device Information.serial number"
        ]["value_raw"]
        df_raw = mdf_file.get_data_frame()

    # extract all DBC decodable signals and print dataframe
    df_phys = df_decoder.decode_frame(df_raw)
    print(f"Extracted {len(df_phys)} DBC decoded frames")
    path = log_file.split("LOG/")[1].replace("MF4", "csv").replace("/", "_")
    df_phys.to_csv(path)

    if df_phys.empty:
        continue

    # group the data to enable a signal-by-signal loop
    df_phys_grouped = df_phys.groupby("Signal")["Physical Value"]

    # for each signal perform some processing
    for signal, signal_data in df_phys_grouped:
        print(f"- {signal}: {len(signal_data)} frames")
        # print(signal_data)

import mdf_iter
import canedge_browser
import can_decoder

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from utils import setup_fs

# specify which devices to process (from local folder or S3 bucket)
devices = ["LOG/958D2219"]

# specify which time period to fetch log files for
start = datetime(year=2020, month=1, day=13, hour=0, minute=0, tzinfo=timezone.utc)
stop = datetime(year=2099, month=1, day=1, tzinfo=timezone.utc)

# specify list of DBC paths and optionally a list of signals to process
base_path = Path(__file__).parent
dbc_paths = [base_path / r"dbc_files/CSS-Electronics-SAE-J1939-DEMO.dbc"]
signal_list = []

# ---------------------------------------------------
# initialize file loader and list log files
fs = setup_fs(s3=False)

log_files = canedge_browser.get_log_files(fs, devices, start_date=start, stop_date=stop)
print(f"Found a total of {len(log_files)} log files")
df_concat = []

# -----------------------------------------
# loop through DBC files, log files and CAN channels
for log_file in log_files:
    # open log file, get device id and extract dataframe with raw CAN data
    print(f"\nProcessing log file: {log_file}")
    with fs.open(log_file, "rb") as handle:
        mdf_file = mdf_iter.MdfFile(handle)
        device_id = mdf_file.get_metadata()["HDComment.Device Information.serial number"]["value_raw"]
        df_raw = mdf_file.get_data_frame()

    for dbc_path in dbc_paths:
        db = can_decoder.load_dbc(dbc_path)
        df_decoder = can_decoder.DataFrameDecoder(db)

        # extract all DBC decodable signals and print dataframe
        df_phys = df_decoder.decode_frame(df_raw)

        if len(signal_list):
            df_phys = df_phys[df_phys["Signal"].isin(signal_list)]

        print(f"Extracted {len(df_phys)} DBC decoded frames")
        path = device_id + log_file.split(device_id)[1].replace("MF4", "csv").replace("/", "_")

        if df_phys.empty:
            continue

        df_phys.to_csv(base_path / path)

        # create a list of the individual DBC decoded dataframes:
        df_concat.append(df_phys)

        # group the data to enable a signal-by-signal loop
        df_phys_grouped = df_phys.groupby("Signal")["Physical Value"]

        # for each signal perform some processing
        for signal, signal_data in df_phys_grouped:
            print(f"- {signal}: {len(signal_data)} frames")
            # print(signal_data)

# create a concatenated dataframe based on the individual dataframes
df_concat = pd.concat(df_concat)
print(f"\nConcatenated all {len(df_concat)} decoded frames into one dataframe")

# -----------------------------------------
# restructure dataframe to have resampled signals in columns & save as CSV
df_join = pd.DataFrame({"TimeStamp": []})
for signal, signal_data in df_concat.groupby("Signal"):
    df_join = pd.merge_ordered(
        df_join, signal_data["Physical Value"].rename(signal).resample("1S").pad().dropna(), on="TimeStamp", fill_method="none",
    )

df_join.set_index("TimeStamp").to_csv(base_path / "output_joined.csv")

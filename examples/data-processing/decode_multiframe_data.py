import mdf_iter, canedge_browser, can_decoder
import sys
import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, MultiFrameDecoder

devices = ["LOG_TP/0D2C6546"]
start = datetime(year=2020, month=1, day=13, hour=0, minute=0, tzinfo=timezone.utc)
dbc_path = r"transport_protocol.dbc"
res_id_list_hex = ["0x7E9"]


# ---------------------------------------------------
# initialize DBC converter and file loader
db = can_decoder.load_dbc(dbc_path)
df_decoder = can_decoder.DataFrameDecoder(db)
fs = setup_fs()

# List log files based on inputs, select first log file and load raw data
log_files = canedge_browser.get_log_files(fs, devices, start_date=start)
log_file = log_files[0]

with fs.open(log_file, "rb") as handle:
    mdf_file = mdf_iter.MdfFile(handle)
    df_raw = mdf_file.get_data_frame()

# replace transport protocol sequences with single frames
tp_decoder = MultiFrameDecoder(df_raw, res_id_list_hex)
df_raw_combined = tp_decoder.combine_multiframes()

# decode the data using multiplexing DBC (similar to OBD2 logic):
df_phys = tp_decoder.decode_multiframe_data(df_raw_combined, df_decoder)

# save data to CSV
df_raw.to_csv("raw_data.csv")
df_raw_combined.to_csv("raw_data_incl_tp.csv")
df_phys.to_csv("physical_values_incl_tp.csv")

print(df_phys)

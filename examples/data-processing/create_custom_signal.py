import mdf_iter
import canedge_browser
import can_decoder

import pandas as pd

from datetime import datetime, timezone
from utils import setup_fs, custom_sig


# specify which devices to process (from local folder or S3 bucket)
devices = ["LOG/958D2219"]

# specify which time period to fetch log files for
start = datetime(year=2020, month=1, day=13, hour=0, minute=0, tzinfo=timezone.utc)
stop = datetime(year=2099, month=1, day=1, tzinfo=timezone.utc)

# specify DBC path
dbc_path = r"dbc_files/CSS-Electronics-SAE-J1939-DEMO.dbc"

# ---------------------------------------------------
# initialize DBC converter and file loader
db = can_decoder.load_dbc(dbc_path)
df_decoder = can_decoder.DataFrameDecoder(db)

fs = setup_fs()

# List log files based on inputs and select first log file
log_files = canedge_browser.get_log_files(fs, devices, start_date=start, stop_date=stop)
log_file = log_files[0]

with fs.open(log_file, "rb") as handle:
    mdf_file = mdf_iter.MdfFile(handle)
    df_raw = mdf_file.get_data_frame()

# extract all DBC decodable signals and print dataframe
df_phys = df_decoder.decode_frame(df_raw)


# define a function for calculating the custom signal
def ratio(s1, s2):
    if s2 != 0:
        return 100 * s1 / s2
    else:
        return np.nan


# calculate custom signal and append to main dataframe, then save as CSV
df_ratio = custom_sig(df_phys, "WheelBasedVehicleSpeed", "EngineSpeed", ratio, "Ratio")

df_phys = df_phys.append(df_ratio)
df_phys.to_csv("data_incl_custom_signal.csv")
print("Saved data incl. custom signal to CSV")

import mdf_iter
import canedge_browser

import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, load_dbc_files, restructure_data, add_custom_sig, ProcessData, test_signal_threshold

# specify devices to process (from local/S3), DBC files, start time and optionally passwords
devices = ["LOG/958D2219"]

dbc_paths = ["dbc_files/CSS-Electronics-SAE-J1939-DEMO.dbc"]
start = datetime(year=2020, month=1, day=13, hour=0, tzinfo=timezone.utc)
pw = {"default": "password"}

# setup filesystem (local/S3), load DBC files and list log files for processing
fs = setup_fs(s3=False, key="", secret="", endpoint="", passwords=pw)
db_list = load_dbc_files(dbc_paths)
log_files = canedge_browser.get_log_files(fs, devices, start_date=start, passwords=pw)
print(f"Found a total of {len(log_files)} log files")

# --------------------------------------------
# perform data processing of each log file (e.g. evaluation of signal stats vs. thresholds)
proc = ProcessData(fs, db_list, signals=[])
df_phys_all = pd.DataFrame()

for log_file in log_files:
    df_raw, device_id = proc.get_raw_data(log_file, passwords=pw)
    df_phys = proc.extract_phys(df_raw)
    proc.print_log_summary(device_id, log_file, df_phys)

    test_signal_threshold(df_phys=df_phys, signal="EngineSpeed", threshold=800)

    df_phys_all = df_phys_all.append(df_phys)


# --------------------------------------------
# example: Add a custom signal
def ratio(s1, s2):
    return s2 / s1 if s1 else np.nan


# df_phys_all = add_custom_sig(df_phys_all, "WheelBasedVehicleSpeed", "EngineSpeed", ratio, "RatioRpmSpeed")

# --------------------------------------------
# example: resample and restructure data (parameters in columns)
df_phys_join = restructure_data(df_phys=df_phys_all, res="1S", full_col_names=True)
df_phys_join.to_csv("output_joined.csv")
print("\nConcatenated DBC decoded data:\n", df_phys_join)

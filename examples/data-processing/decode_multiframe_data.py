import mdf_iter, canedge_browser, can_decoder, os, sys
import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, MultiFrameDecoder

# pd.set_option("display.max_columns", 500)

# ---------------------------------------------------
# initialize DBC converter and file loader
def process_tp_example(devices, dbc_path, res_id_list_hex, tp_type):

    db = can_decoder.load_dbc(dbc_path)
    df_decoder = can_decoder.DataFrameDecoder(db)
    fs = setup_fs()

    # List log files based on inputs, select first log file and load raw data
    log_files = canedge_browser.get_log_files(fs, devices)

    for log_file in log_files:
        print(f"Processing {log_file}")
        with fs.open(log_file, "rb") as handle:
            mdf_file = mdf_iter.MdfFile(handle)
            df_raw = mdf_file.get_data_frame()

        # replace transport protocol sequences with single frames
        tp = MultiFrameDecoder(df_raw, res_id_list_hex)

        if tp_type == "uds":
            df_raw_combined = tp.combine_tp_frames_uds()
        elif tp_type == "nmea":
            df_raw_combined = tp.combine_tp_frames_nmea()

        # decode the data using multiplexing DBC (similar to OBD2 logic):
        df_phys = tp.decode_tp_data(df_raw_combined, df_decoder)

        # save data to CSV
        output_folder = "output" + log_file.replace(".MF4", "")

        if not os.path.exists(output_folder):
            os.makedirs(f"{output_folder}")

        df_raw.to_csv(f"{output_folder}/raw_data.csv")
        df_raw_combined.to_csv(f"{output_folder}/raw_incl_tp.csv")
        df_phys.to_csv(f"{output_folder}/phys_incl_tp.csv")
        print(f"Data saved to {output_folder}")


# ----------------------------------------

# run examples
devices = ["LOG_TP/0D2C6546"]
dbc_path = r"tp_uds.dbc"
res_id_list_hex = ["0x7E9"]

process_tp_example(devices, dbc_path, res_id_list_hex, "uds")

devices = ["LOG_TP/64AB4329"]
res_id_list_hex = ["0x1DEFFF00"]
dbc_path = r"tp_nmea.dbc"

process_tp_example(devices, dbc_path, res_id_list_hex, "nmea")

# devices = ["LOG_TP/FE34E37D"]
# res_id_list_hex = ["0x7E9"]
# dbc_path = r"tp_vin.dbc"
#
# process_tp_example(devices, dbc_path, res_id_list_hex, "uds")

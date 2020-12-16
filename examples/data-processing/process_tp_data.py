import mdf_iter, canedge_browser, can_decoder, os
import pandas as pd
from datetime import datetime, timezone
from utils import setup_fs, load_dbc_files, ProcessData
from utils_tp import MultiFrameDecoder

# ---------------------------------------------------
# initialize DBC converter and file loader
def process_tp_example(devices, dbc_path, res_id_list_hex, tp_type):
    fs = setup_fs(s3=False)
    db_list = load_dbc_files(dbc_paths)
    log_files = canedge_browser.get_log_files(fs, devices)

    proc = ProcessData(fs, db_list)

    for log_file in log_files:
        # create output folder
        output_folder = "output" + log_file.replace(".MF4", "")
        if not os.path.exists(output_folder):
            os.makedirs(f"{output_folder}")

        df_raw, device_id = proc.get_raw_data(log_file)
        df_raw.to_csv(f"{output_folder}/tp_raw_data.csv")

        # replace transport protocol sequences with single frames
        tp = MultiFrameDecoder(df_raw, res_id_list_hex)
        df_raw = tp.combine_tp_frames_by_type(tp_type)
        df_raw.to_csv(f"{output_folder}/tp_raw_data_combined.csv")

        # extract physical values as normal
        df_phys = proc.extract_phys(df_raw, tp_type=tp_type)
        df_phys.to_csv(f"{output_folder}/tp_physical_values.csv")

    print("Finished saving CSV output for devices:", devices)


# ----------------------------------------
# run different TP examples

# basic UDS example with multiple UDS PIDs on same CAN ID, e.g. 221100, 221101
devices = ["LOG_TP/0D2C6546"]
dbc_paths = [r"dbc_files/tp_uds_test.dbc"]
res_id_list_hex = ["0x7E9"]

process_tp_example(devices, dbc_paths, res_id_list_hex, "uds")

# UDS data from Hyundai Kona EV (SoC%)
devices = ["LOG_TP/17BD1DB7"]
dbc_paths = [r"dbc_files/tp_uds_hyundai_soc.dbc"]
res_id_list_hex = ["0x7EC", "0x7BB"]

process_tp_example(devices, dbc_paths, res_id_list_hex, "uds")

# J1939 TP data
devices = ["LOG_TP/FCBF0606"]
res_id_list_hex = ["0x1CEBFF00"]
dbc_paths = [r"dbc_files/tp_j1939.dbc"]

process_tp_example(devices, dbc_paths, res_id_list_hex, "j1939")

# UDS data across two CAN channels
# devices = ["LOG_TP/FE34E37D"]
# dbc_paths = [r"dbc_files/tp_uds_test.dbc"]
# res_id_list_hex = ["0x7EA"]
#
# process_tp_example(devices, dbc_paths, res_id_list_hex, "uds")

# NMEA 2000 TP data
# devices = ["LOG_TP/64AB4329"]
# res_id_list_hex = ["0x1DEFFF00"]
# dbc_paths = [r"dbc_files/tp_nmea.dbc"]
#
# process_tp_example(devices, dbc_paths, res_id_list_hex, "nmea")

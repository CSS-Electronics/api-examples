import canedge_browser, os
from utils import setup_fs, load_dbc_files, ProcessData, MultiFrameDecoder


def process_tp_example(devices, dbc_path, tp_type):
    fs = setup_fs(s3=False)
    db_list = load_dbc_files(dbc_paths)
    log_files = canedge_browser.get_log_files(fs, devices)

    proc = ProcessData(fs, db_list)

    for log_file in log_files:
        output_folder = "output" + log_file.replace(".MF4", "")
        if not os.path.exists(output_folder):
            os.makedirs(f"{output_folder}")

        df_raw, device_id = proc.get_raw_data(log_file)
        df_raw.to_csv(f"{output_folder}/tp_raw_data.csv")

        # replace transport protocol sequences with single frames
        tp = MultiFrameDecoder(tp_type)
        df_raw = tp.combine_tp_frames(df_raw)
        df_raw.to_csv(f"{output_folder}/tp_raw_data_combined.csv")

        # extract physical values as normal, but add tp_type
        df_phys = proc.extract_phys(df_raw)
        df_phys.to_csv(f"{output_folder}/tp_physical_values.csv")

    print("Finished saving CSV output for devices:", devices)


# ----------------------------------------
# run different TP examples

# UDS data from Hyundai Kona EV (SoC%)
devices = ["LOG_TP/17BD1DB7"]
dbc_paths = [r"dbc_files/tp_uds_hyundai_soc.dbc"]
process_tp_example(devices, dbc_paths, "uds")

# J1939 TP data
devices = ["LOG_TP/FCBF0606"]
dbc_paths = [r"dbc_files/tp_j1939.dbc"]
process_tp_example(devices, dbc_paths, "j1939")

# NMEA 2000 fast packet data (with GNSS position)
devices = ["LOG_TP/94C49784"]
dbc_paths = [r"dbc_files/tp_nmea_2.dbc"]
process_tp_example(devices, dbc_paths, "nmea")

# UDS data across two CAN channels
devices = ["LOG_TP/FE34E37D"]
dbc_paths = [r"dbc_files/tp_uds_test.dbc"]
process_tp_example(devices, dbc_paths, "uds")

# UDS example with multiple UDS PIDs on same CAN ID, e.g. 221100, 221101
devices = ["LOG_TP/0D2C6546"]
dbc_paths = [r"dbc_files/tp_uds_test.dbc"]
process_tp_example(devices, dbc_paths, "uds")

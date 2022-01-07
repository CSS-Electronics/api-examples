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
devices = ["LOG/17BD1DB7"]
dbc_paths = [r"dbc_files/tp_uds.dbc"]
process_tp_example(devices, dbc_paths, "uds")

# UDS data from Nissan Leaf 2019 (SoC%)
devices = ["LOG/2F6913DB"]
dbc_paths = [r"dbc_files/tp_uds_nissan.dbc"]
process_tp_example(devices, dbc_paths, "uds")

# J1939 TP data
devices = ["LOG/FCBF0606"]
dbc_paths = [r"dbc_files/tp_j1939.dbc"]
process_tp_example(devices, dbc_paths, "j1939")

# NMEA 2000 fast packet data (with GNSS position)
devices = ["LOG/94C49784"]
dbc_paths = [r"dbc_files/tp_nmea.dbc"]
process_tp_example(devices, dbc_paths, "nmea")

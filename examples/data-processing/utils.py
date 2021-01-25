def setup_fs(s3, key="", secret="", endpoint="", cert=""):
    """Given a boolean specifying whether to use local disk or S3, setup filesystem
    Syntax examples: AWS (http://s3.us-east-2.amazonaws.com), MinIO (http://192.168.0.1:9000)
    The cert input is relevant if you're using MinIO with TLS enabled, for specifying the path to the certficiate
    """

    if s3:
        import s3fs

        if "amazonaws" in endpoint:
            fs = s3fs.S3FileSystem(key=key, secret=secret)
        elif cert != "":
            fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs={"endpoint_url": endpoint, "verify": cert})
        else:
            fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs={"endpoint_url": endpoint},)

    else:
        from pathlib import Path
        import canedge_browser

        base_path = Path(__file__).parent
        fs = canedge_browser.LocalFileSystem(base_path=base_path)

    return fs


# -----------------------------------------------
def load_dbc_files(dbc_paths):
    """Given a list of DBC file paths, create a list of conversion rule databases
    """
    import can_decoder
    from pathlib import Path

    db_list = []
    for dbc in dbc_paths:
        db = can_decoder.load_dbc(Path(__file__).parent / dbc)
        db_list.append(db)

    return db_list


# -----------------------------------------------
def list_log_files(fs, devices, start_times, verbose=True):
    """Given a list of device paths, list log files from specified filesystem.
    Data is loaded based on the list of start datetimes
    """
    import canedge_browser, mdf_iter

    log_files = []

    if len(start_times):
        for idx, device in enumerate(devices):
            start = start_times[idx]
            log_files_device = canedge_browser.get_log_files(fs, [device], start_date=start)

            # exclude the 1st log file if the last timestamp is before the start timestamp
            if len(log_files_device) > 0:
                with fs.open(log_files_device[0], "rb") as handle:
                    mdf_file = mdf_iter.MdfFile(handle)
                    df_raw = mdf_file.get_data_frame()
                    end_time = df_raw.index[-1]

                if end_time < start:
                    log_files_device = log_files_device[1:]

                log_files.extend(log_files_device)

    if verbose:
        print(f"Found {len(log_files)} log files\n")

    return log_files


def restructure_data(df_phys, res):
    import pandas as pd

    df_phys_join = pd.DataFrame({"TimeStamp": []})
    if not df_phys.empty:
        for signal, data in df_phys.groupby("Signal"):
            df_phys_join = pd.merge_ordered(
                df_phys_join,
                data["Physical Value"].rename(signal).resample("1S").pad().dropna(),
                on="TimeStamp",
                fill_method="none",
            ).set_index("TimeStamp")

        df_phys_join.to_csv("output_joined.csv")

    return df_phys_join


def add_custom_sig(df_phys, signal1, signal2, function, new_signal):
    """Helper function for calculating a new signal based on two signals and a function.
    Returns a dataframe with the new signal name and physical values
    """
    import pandas as pd

    try:
        s1 = df_phys[df_phys["Signal"] == signal1]["Physical Value"].rename(signal1)
        s2 = df_phys[df_phys["Signal"] == signal2]["Physical Value"].rename(signal2)

        df_new_sig = pd.merge_ordered(s1, s2, on="TimeStamp", fill_method="ffill",).set_index("TimeStamp")
        df_new_sig = df_new_sig.apply(lambda x: function(x[0], x[1]), axis=1).dropna().rename("Physical Value").to_frame()
        df_new_sig["Signal"] = new_signal
        df_phys = df_phys.append(df_new_sig)

    except:
        print(f"Warning: Custom signal {new_signal} not created\n")

    return df_phys


# -----------------------------------------------
class ProcessData:
    def __init__(self, fs, db_list, signals=[], days_offset=None, verbose=True):
        self.db_list = db_list
        self.signals = signals
        self.fs = fs
        self.days_offset = days_offset
        self.verbose = verbose
        return

    def extract_phys(self, df_raw, tp_type=None):
        """Given df of raw data and list of decoding databases, create new def with
        physical values (no duplicate signals and optionally filtered/rebaselined)
        """
        import can_decoder
        import pandas as pd

        df_phys = pd.DataFrame()
        for db in self.db_list:
            df_decoder = can_decoder.DataFrameDecoder(db)

            if tp_type != None:
                df_phys_tp = pd.DataFrame()
                for length, group in df_raw.groupby("DataLength"):
                    df_phys_group = df_decoder.decode_frame(group)
                    df_phys_tp = df_phys_tp.append(df_phys_group)

                df_phys = df_phys.append(df_phys_tp.sort_index())
            else:
                df_phys = df_phys.append(df_decoder.decode_frame(df_raw))

        # remove duplicates in case multiple DBC files contain identical signals
        df_phys["datetime"] = df_phys.index
        df_phys = df_phys.drop_duplicates(keep="first")
        df_phys = df_phys.drop("datetime", 1)

        # optionally filter and rebaseline the data
        df_phys = self.filter_signals(df_phys)
        df_phys = self.rebaseline_data(df_phys)

        return df_phys

    def rebaseline_data(self, df_phys):
        """Given a df of physical values, this offsets the timestamp
        to be equal to today, minus a given number of days.
        """
        if not df_phys.empty and type(self.days_offset) == int:
            from datetime import datetime, timezone

            delta_days = (datetime.now(timezone.utc) - df_phys.index.min()).days - self.days_offset
            df_phys.index = df_phys.index + pd.Timedelta(delta_days, "day")

        return df_phys

    def filter_signals(self, df_phys):
        """Given a df of physical values, return only signals matched by filter
        """
        if not df_phys.empty and len(self.signals):
            df_phys = df_phys[df_phys["Signal"].isin(self.signals)]

        return df_phys

    def get_raw_data(self, log_file):
        """Extract a df of raw data and device ID from log file
        """
        import mdf_iter

        with self.fs.open(log_file, "rb") as handle:
            mdf_file = mdf_iter.MdfFile(handle)
            device_id = self.get_device_id(mdf_file)
            df_raw_lin = mdf_file.get_data_frame_lin()
            df_raw_lin["IDE"] = 0
            df_raw_can = mdf_file.get_data_frame()
            df_raw = df_raw_can.append(df_raw_lin)

        return df_raw, device_id

    def get_device_id(self, mdf_file):
        return mdf_file.get_metadata()["HDComment.Device Information.serial number"]["value_raw"]

    def print_log_summary(self, device_id, log_file, df_phys):
        """Print summary information for each log file
        """
        if self.verbose:
            print(
                "\n---------------",
                f"\nDevice: {device_id} | Log file: {log_file.split(device_id)[-1]} [Extracted {len(df_phys)} decoded frames]\nPeriod: {df_phys.index.min()} - {df_phys.index.max()}\n",
            )

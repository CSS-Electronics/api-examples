def setup_fs(s3, key="", secret="", endpoint="", cert="", passwords={}):
    """Given a boolean specifying whether to use local disk or S3, setup filesystem
    Syntax examples: AWS (http://s3.us-east-2.amazonaws.com), MinIO (http://192.168.0.1:9000)
    The cert input is relevant if you're using MinIO with TLS enabled, for specifying the path to the certficiate.

    The block_size is set to accomodate files up to 55 MB in size. If your log files are larger, adjust this value accordingly
    """

    if s3:
        import s3fs

        block_size = 55 * 1024 * 1024

        if "amazonaws" in endpoint:
            fs = s3fs.S3FileSystem(key=key, secret=secret, default_block_size=block_size)
        elif cert != "":
            fs = s3fs.S3FileSystem(
                key=key,
                secret=secret,
                client_kwargs={"endpoint_url": endpoint, "verify": cert},
                default_block_size=block_size,
            )
        else:
            fs = s3fs.S3FileSystem(
                key=key,
                secret=secret,
                client_kwargs={"endpoint_url": endpoint},
                default_block_size=block_size,
            )

    else:
        from pathlib import Path
        import canedge_browser

        base_path = Path(__file__).parent
        fs = canedge_browser.LocalFileSystem(base_path=base_path, passwords=passwords)

    return fs


# -----------------------------------------------
def load_dbc_files(dbc_paths):
    """Given a list of DBC file paths, create a list of conversion rule databases"""
    import can_decoder
    from pathlib import Path

    db_list = []
    for dbc in dbc_paths:
        db = can_decoder.load_dbc(Path(__file__).parent / dbc)
        db_list.append(db)

    return db_list


# -----------------------------------------------
def list_log_files(fs, devices, start_times, verbose=True, passwords={}):
    """Given a list of device paths, list log files from specified filesystem.
    Data is loaded based on the list of start datetimes
    """
    import canedge_browser, mdf_iter

    log_files = []

    if len(start_times):
        for idx, device in enumerate(devices):
            start = start_times[idx]
            log_files_device = canedge_browser.get_log_files(fs, [device], start_date=start, passwords=passwords)
            log_files.extend(log_files_device)

    if verbose:
        print(f"Found {len(log_files)} log files\n")

    return log_files


def restructure_data(df_phys, res, full_col_names=False, pgn_names=False):
    import pandas as pd
    from J1939_PGN import J1939_PGN

    df_phys_join = pd.DataFrame({"TimeStamp": []})
    if not df_phys.empty:
        for message, df_phys_message in df_phys.groupby("CAN ID"):
            for signal, data in df_phys_message.groupby("Signal"):

                pgn = J1939_PGN(int(message)).pgn

                if full_col_names == True and pgn_names == False:
                    col_name = str(hex(int(message))).upper()[2:] + "." + signal
                elif full_col_names == True and pgn_names == True:
                    col_name = str(hex(int(message))).upper()[2:] + "." + str(pgn) + "." + signal
                elif full_col_names == False and pgn_names == True:
                    col_name = str(pgn) + "." + signal
                else:
                    col_name = signal

                df_phys_join = pd.merge_ordered(
                    df_phys_join,
                    data["Physical Value"].rename(col_name).resample(res).pad().dropna(),
                    on="TimeStamp",
                    fill_method="none",
                ).set_index("TimeStamp")

    return df_phys_join


def test_signal_threshold(df_phys, signal, threshold):
    """Illustrative example for how to extract a signal and evaluate statistical values
    vs. defined thresholds. The function can be easily modified for your needs.
    """
    df_signal = df_phys[df_phys["Signal"] == signal]["Physical Value"]

    stats = df_signal.agg(["count", "min", "max", "mean", "std"])
    delta = stats["max"] - stats["min"]

    if delta > threshold:
        print(f"{signal} exhibits a 'max - min' delta of {delta} exceeding threshold of {threshold}")


def add_custom_sig(df_phys, signal1, signal2, function, new_signal):
    """Helper function for calculating a new signal based on two signals and a function.
    Returns a dataframe with the new signal name and physical values
    """
    import pandas as pd

    try:
        s1 = df_phys[df_phys["Signal"] == signal1]["Physical Value"].rename(signal1)
        s2 = df_phys[df_phys["Signal"] == signal2]["Physical Value"].rename(signal2)

        df_new_sig = pd.merge_ordered(
            s1,
            s2,
            on="TimeStamp",
            fill_method="ffill",
        ).set_index("TimeStamp")
        df_new_sig = df_new_sig.apply(lambda x: function(x[0], x[1]), axis=1).dropna().rename("Physical Value").to_frame()
        df_new_sig["Signal"] = new_signal
        df_phys = df_phys.append(df_new_sig)

    except:
        print(f"Warning: Custom signal {new_signal} not created\n")

    return df_phys


# -----------------------------------------------
class ProcessData:
    def __init__(self, fs, db_list, signals=[], days_offset=None, verbose=True):
        from datetime import datetime, timedelta

        self.db_list = db_list
        self.signals = signals
        self.fs = fs
        self.days_offset = days_offset
        self.verbose = verbose

        if self.verbose == True and self.days_offset != None:
            date_offset = (datetime.today() - timedelta(days=self.days_offset)).strftime("%Y-%m-%d")
            print(
                f"Warning: days_offset = {self.days_offset}, meaning data is offset to start at {date_offset}.\nThis is intended for sample data testing only. Set days_offset = None when processing your own data."
            )

        return

    def extract_phys(self, df_raw):
        """Given df of raw data and list of decoding databases, create new def with
        physical values (no duplicate signals and optionally filtered/rebaselined)
        """
        import can_decoder
        import pandas as pd

        df_phys = pd.DataFrame()
        for db in self.db_list:
            df_decoder = can_decoder.DataFrameDecoder(db)

            df_phys_temp = pd.DataFrame()
            for length, group in df_raw.groupby("DataLength"):
                df_phys_group = df_decoder.decode_frame(group)
                df_phys_temp = df_phys_temp.append(df_phys_group)

            df_phys = df_phys.append(df_phys_temp.sort_index())

        # remove duplicates in case multiple DBC files contain identical signals
        df_phys["datetime"] = df_phys.index
        df_phys = df_phys.drop_duplicates(keep="first")
        df_phys = df_phys.drop(labels="datetime", axis=1)

        # optionally filter and rebaseline the data
        df_phys = self.filter_signals(df_phys)

        if not df_phys.empty and type(self.days_offset) == int:
            df_phys = self.rebaseline_data(df_phys)

        return df_phys

    def rebaseline_data(self, df_phys):
        """Given a df of physical values, this offsets the timestamp
        to be equal to today, minus a given number of days.
        """
        from datetime import datetime, timezone
        import pandas as pd

        delta_days = (datetime.now(timezone.utc) - df_phys.index.min()).days - self.days_offset
        df_phys.index = df_phys.index + pd.Timedelta(delta_days, "day")

        return df_phys

    def filter_signals(self, df_phys):
        """Given a df of physical values, return only signals matched by filter"""
        if not df_phys.empty and len(self.signals):
            df_phys = df_phys[df_phys["Signal"].isin(self.signals)]

        return df_phys

    def get_raw_data(self, log_file, lin=False, passwords={}):
        """Extract a df of raw data and device ID from log file.
        Optionally include LIN bus data by setting lin=True
        """
        import mdf_iter

        with self.fs.open(log_file, "rb") as handle:
            mdf_file = mdf_iter.MdfFile(handle, passwords=passwords)
            device_id = self.get_device_id(mdf_file)

            if lin:
                df_raw_lin = mdf_file.get_data_frame_lin()
                df_raw_lin["IDE"] = 0
                df_raw_can = mdf_file.get_data_frame()
                df_raw = df_raw_can.append(df_raw_lin)
            else:
                df_raw = mdf_file.get_data_frame()

        return df_raw, device_id

    def get_device_id(self, mdf_file):
        return mdf_file.get_metadata()["HDComment.Device Information.serial number"]["value_raw"]

    def print_log_summary(self, device_id, log_file, df_phys):
        """Print summary information for each log file"""
        if self.verbose:
            print(
                "\n---------------",
                f"\nDevice: {device_id} | Log file: {log_file.split(device_id)[-1]} [Extracted {len(df_phys)} decoded frames]\nPeriod: {df_phys.index.min()} - {df_phys.index.max()}\n",
            )


# -----------------------------------------------
class MultiFrameDecoder:
    """BETA class for handling transport protocol data. For each response ID, identify
    sequences of subsequent frames and combine the relevant parts of the data payloads
    into a single payload with the response ID as the ID. The original raw dataframe is
    then cleansed of the original response ID sequence frames. Instead, the new concatenated
    frames are inserted. Further, the class supports DBC decoding of the resulting modified raw data

    :param tp_type:                     the class supports UDS ("uds"), NMEA 2000 Fast Packets ("nmea") and J1939 ("j1939")
    :param df_raw:                      dataframe of raw CAN data from the mdf_iter module

    SINGLE_FRAME_MASK:                  mask used in matching single frames
    FIRST_FRAME_MASK:                   mask used in matching first frames
    CONSEQ_FRAME_MASK:                  mask used in matching consequtive frames
    SINGLE_FRAME:                       frame type reflecting a single frame response
    FIRST_FRAME:                        frame type reflecting the first frame in a multi frame response
    CONSEQ_FRAME:                       frame type reflecting a consequtive frame in a multi frame response
    ff_payload_start:                   the combined payload will start at this byte in the FIRST_FRAME
    bam_pgn:                            this is used in J1939 and marks the initial BAM message ID in DEC
    res_id_list_hex:                    TP 'response CAN IDs' to process. For nmea/j1939, these are provided by default

    """

    def __init__(self, tp_type=""):
        frame_struct_uds = {
            "SINGLE_FRAME_MASK": 0xF0,
            "FIRST_FRAME_MASK": 0xF0,
            "CONSEQ_FRAME_MASK": 0xF0,
            "SINGLE_FRAME": 0x00,
            "FIRST_FRAME": 0x10,
            "CONSEQ_FRAME": 0x20,
            "ff_payload_start": 2,
            "bam_pgn": -1,
            "res_id_list_hex": [
                "0x7E0",
                "0x7E9",
                "0x7EA",
                "0x7EB",
                "0x7EC",
                "0x7ED",
                "0x7EE",
                "0x7EF",
                "0x7EA",
                "0x7BB",
            ],
        }

        frame_struct_j1939 = {
            "SINGLE_FRAME_MASK": 0xFF,
            "FIRST_FRAME_MASK": 0xFF,
            "CONSEQ_FRAME_MASK": 0x00,
            "SINGLE_FRAME": 0xFF,
            "FIRST_FRAME": 0x20,
            "CONSEQ_FRAME": 0x00,
            "ff_payload_start": 8,
            "bam_pgn": int("0xEC00", 16),
            "res_id_list_hex": ["0xEB00"],
        }

        frame_struct_nmea = {
            "SINGLE_FRAME_MASK": 0xFF,
            "FIRST_FRAME_MASK": 0x1F,
            "CONSEQ_FRAME_MASK": 0x00,
            "SINGLE_FRAME": 0xFF,
            "FIRST_FRAME": 0x00,
            "CONSEQ_FRAME": 0x00,
            "ff_payload_start": 2,
            "bam_pgn": -1,
            "res_id_list_hex": [
                "0xfed8",
                "0x1f007",
                "0x1f008",
                "0x1f009",
                "0x1f014",
                "0x1f016",
                "0x1f101",
                "0x1f105",
                "0x1f201",
                "0x1f208",
                "0x1f209",
                "0x1f20a",
                "0x1f20c",
                "0x1f20f",
                "0x1f210",
                "0x1f212",
                "0x1f513",
                "0x1f805",
                "0x1f80e",
                "0x1f80f",
                "0x1f810",
                "0x1f811",
                "0x1f814",
                "0x1f815",
                "0x1f904",
                "0x1f905",
                "0x1fa04",
                "0x1fb02",
                "0x1fb03",
                "0x1fb04",
                "0x1fb05",
                "0x1fb11",
                "0x1fb12",
                "0x1fd10",
                "0x1fe07",
                "0x1fe12",
                "0x1ff14",
                "0x1ff15",
            ],
        }

        if tp_type == "uds":
            self.frame_struct = frame_struct_uds
        elif tp_type == "j1939":
            self.frame_struct = frame_struct_j1939
        elif tp_type == "nmea":
            self.frame_struct = frame_struct_nmea
        else:
            self.frame_struct = {}

        self.tp_type = tp_type

        return

    def calculate_pgn(self, frame_id):
        pgn = (frame_id & 0x03FFFF00) >> 8

        pgn_f = (pgn & 0xFF00) >> 8
        pgn_s = pgn & 0x00FF

        if pgn_f < 240:
            pgn &= 0xFFFFFF00

        return pgn

    def calculate_sa(self, frame_id):
        sa = frame_id & 0x000000FF

        return sa

    def construct_new_tp_frame(self, base_frame, payload_concatenated, can_id):
        new_frame = base_frame
        new_frame.at["DataBytes"] = payload_concatenated
        new_frame.at["DLC"] = 0
        new_frame.at["DataLength"] = len(payload_concatenated)

        if can_id:
            new_frame.at["ID"] = can_id

        return new_frame

    def combine_tp_frames(self, df_raw):
        import pandas as pd

        bam_pgn = self.frame_struct["bam_pgn"]
        res_id_list = [int(res_id, 16) for res_id in self.frame_struct["res_id_list_hex"]]

        df_list_combined = []

        # use PGN matching for J1939 and NMEA and update res_id_list to relevant entries
        if self.tp_type == "nmea" or self.tp_type == "j1939":
            res_id_list_incl_bam = res_id_list
            res_id_list_incl_bam.append(bam_pgn)
            df_raw_match = df_raw["ID"].apply(self.calculate_pgn).isin(res_id_list_incl_bam)
            res_id_list = df_raw["ID"][df_raw_match].apply(self.calculate_pgn).drop_duplicates().values.tolist()

            df_raw_tp = df_raw[df_raw_match]
            df_raw_excl_tp = df_raw[~df_raw_match]
        else:
            df_raw_match = df_raw["ID"].isin(res_id_list)
            res_id_list = df_raw["ID"][df_raw_match].drop_duplicates().values.tolist()

            df_raw_tp = df_raw[df_raw_match]
            df_raw_excl_tp = df_raw[~df_raw["ID"].isin(res_id_list)]

        if len(df_raw.index) - len(df_raw_tp.index) - len(df_raw_excl_tp.index):
            print("Warning - total rows does not equal sum of rows incl/excl transport protocol frames")

        df_list_combined.append(df_raw_excl_tp)

        for res_id in res_id_list:
            # filter raw data for response ID and extract a 'base frame'
            if self.tp_type == "nmea" or self.tp_type == "j1939":
                df_raw_res_id = df_raw_tp[df_raw_tp["ID"].apply(self.calculate_pgn).isin([res_id, bam_pgn])]
                df_raw_res_id = df_raw_res_id.copy()
                df_raw_res_id["SA"] = df_raw_res_id.ID.apply(self.calculate_sa)
            else:
                df_raw_res_id = df_raw_tp[df_raw_tp["ID"].isin([res_id])]

            if df_raw_res_id.empty:
                continue

            for channel, df_channel in df_raw_res_id.groupby("BusChannel"):

                # if J1939, we can't group by CAN ID (as we need both bam_pgn and response)
                if self.tp_type == "j1939":
                    group = "SA"
                else:
                    group = "ID"

                for identifier, df_raw_filter in df_channel.groupby(group):

                    base_frame = df_raw_filter.iloc[0]

                    frame_list = []
                    frame_timestamp_list = []
                    payload_concatenated = []
                    ff_length = 0xFFF
                    can_id = None
                    conseq_frame_prev = None

                    # iterate through rows in filtered dataframe
                    for index, row in df_raw_filter.iterrows():
                        first_byte = row["DataBytes"][0]

                        # check if first frame (either for UDS/NMEA or J1939 case)
                        if self.tp_type == "j1939" and bam_pgn == self.calculate_pgn(row["ID"]):
                            first_frame_test = True
                        elif (first_byte & self.frame_struct["FIRST_FRAME_MASK"]) == self.frame_struct["FIRST_FRAME"]:
                            first_frame_test = True
                        else:
                            first_frame_test = False

                        # if single frame, save frame directly (excl. 1st byte)
                        if self.tp_type != "nmea" and (first_byte & self.frame_struct["SINGLE_FRAME_MASK"] == self.frame_struct["SINGLE_FRAME"]):
                            new_frame = self.construct_new_tp_frame(base_frame, row["DataBytes"], row["ID"])
                            frame_list.append(new_frame.values.tolist())
                            frame_timestamp_list.append(index)

                        # if first frame, save info from prior multi frame response sequence,
                        # then initialize a new sequence incl. the first frame payload
                        elif first_frame_test:
                            # create a new frame using information from previous iterations
                            if len(payload_concatenated) >= ff_length:
                                new_frame = self.construct_new_tp_frame(base_frame, payload_concatenated, can_id)

                                frame_list.append(new_frame.values.tolist())
                                frame_timestamp_list.append(frame_timestamp)

                            # reset and start on next frame
                            payload_concatenated = []
                            conseq_frame_prev = None
                            frame_timestamp = index

                            # for J1939, extract PGN and convert to 29 bit CAN ID for use in baseframe
                            if self.tp_type == "j1939":
                                pgn_hex = "".join("{:02x}".format(x) for x in reversed(row["DataBytes"][5:8]))
                                pgn = int(pgn_hex, 16)
                                can_id = (6 << 26) | (pgn << 8) | 254

                            ff_length = (row["DataBytes"][0] & 0x0F) << 8 | row["DataBytes"][1]

                            for byte in row["DataBytes"][self.frame_struct["ff_payload_start"] :]:
                                payload_concatenated.append(byte)

                        # if consequtive frame, extend payload with payload excl. 1st byte
                        elif first_byte & self.frame_struct["CONSEQ_FRAME_MASK"] == self.frame_struct["CONSEQ_FRAME"]:
                            if (conseq_frame_prev == None) or ((first_byte - conseq_frame_prev) == 1):
                                conseq_frame_prev = first_byte
                                for byte in row["DataBytes"][1:]:
                                    payload_concatenated.append(byte)

                    df_raw_res_id_new = pd.DataFrame(frame_list, columns=base_frame.index, index=frame_timestamp_list)
                    df_list_combined.append(df_raw_res_id_new)

        df_raw_combined = pd.concat(df_list_combined)
        df_raw_combined.index.name = "TimeStamp"
        df_raw_combined = df_raw_combined.sort_index()

        return df_raw_combined

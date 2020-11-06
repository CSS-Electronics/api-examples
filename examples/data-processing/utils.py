def setup_fs_s3():
    """Helper function to setup a remote S3 filesystem connection.
    """
    import s3fs

    fs = s3fs.S3FileSystem(
        key="<key>",
        secret="<secret>",
        client_kwargs={
            "endpoint_url": "<endpoint>",
            # "verify": "path\\to\\public_certificate.crt",  # for TLS enabled MinIO servers
        },
    )

    return fs


def setup_fs():
    """Helper function to setup the file system.
    """
    from pathlib import Path
    import canedge_browser

    base_path = Path(__file__).parent

    # Setup path to local folder structure, as if copied from a CANedge SD.
    # Assumes the folder is placed in same directory as this file
    fs = canedge_browser.LocalFileSystem(base_path=base_path)

    return fs


def custom_sig(df, signal1, signal2, function, new_signal):
    """Helper function for calculating a new signal based on two signals and a function.
    Returns a dataframe with the new signal name and physical values
    """
    import pandas as pd

    try:
        s1 = df[df["Signal"] == signal1]["Physical Value"].rename(signal1)
        s2 = df[df["Signal"] == signal2]["Physical Value"].rename(signal2)

        df_new_sig = pd.merge_ordered(s1, s2, on="TimeStamp", fill_method="ffill",).set_index("TimeStamp")

        df_new_sig = df_new_sig.apply(lambda x: function(x[0], x[1]), axis=1).dropna().rename("Physical Value").to_frame()

        df_new_sig["Signal"] = new_signal

        return df_new_sig

    except:
        print(f"Warning: Custom signal {new_signal} not created\n")
        return pd.DataFrame()


class MultiFrameDecoder:
    """BETA class for handling transport protocol data. For each response ID, identify
    sequences of subsequent frames and combine the relevant parts of the data payloads
    into a single payload with the response ID as the ID. The original raw dataframe is
    then cleansed of the original response ID sequence frames. Instead, the new concatenated
    frames are inserted. Further, the class supports DBC decoding of the resulting modified raw data

    :param df_raw:                      dataframe of raw CAN data from the mdf_iter module
    :param res_id_list_hex:             list of transport protocol 'response CAN IDs' to process
    :param FRAME_TYPE_MASK:             mask used in identifying frame type based on 1st byte
    :param SINGLE_FRAME:                frame type reflecting a single frame response
    :param FIRST_FRAME:                 frame type reflecting the first frame in a multi frame response
    :param CONSEQ_FRAME:                frame type reflecting a consequtive frame in a multi frame response
    :param first_frame_payload_start:   the combined payload will start at this byte in the FIRST_FRAME
    """

    def __init__(
        self,
        df_raw,
        res_id_list_hex,
        FRAME_TYPE_MASK=0xF0,
        SINGLE_FRAME=0x00,
        FIRST_FRAME=0x10,
        CONSEQ_FRAME=0x20,
        first_frame_payload_start=2,
    ):
        self.df_raw = df_raw
        self.res_id_list_hex = res_id_list_hex
        self.FRAME_TYPE_MASK = FRAME_TYPE_MASK
        self.SINGLE_FRAME = SINGLE_FRAME
        self.FIRST_FRAME = FIRST_FRAME
        self.CONSEQ_FRAME = CONSEQ_FRAME
        self.first_frame_payload_start = first_frame_payload_start
        self.res_id_list = [int(res_id, 16) for res_id in self.res_id_list_hex]

        return

    def construct_new_frame(self, base_frame, payload_concatenated):
        new_frame = base_frame
        new_frame.at["DataBytes"] = payload_concatenated
        new_frame.at["DLC"] = 0
        new_frame.at["DataLength"] = len(payload_concatenated)
        return new_frame

    def combine_multiframes(self):
        import pandas as pd

        for res_id in self.res_id_list:
            # filter raw data for response ID and extract a 'base frame'
            df_raw_filter = self.df_raw[self.df_raw["ID"] == res_id]
            base_frame = df_raw_filter.iloc[0]

            frame_list = []
            frame_timestamp_list = []
            payload_concatenated = []

            # iterate through rows in filtered dataframe
            for index, row in df_raw_filter.iterrows():
                payload = row["DataBytes"]
                first_byte = payload[0]

                # if single frame, save frame directly (excl. 1st byte)
                if first_byte & self.FRAME_TYPE_MASK == self.SINGLE_FRAME:
                    new_frame = self.construct_new_frame(base_frame, payload[1:])
                    frame_list.append(new_frame.values.tolist())
                    frame_timestamp_list.append(index)

                # if first frame, save info from prior multi frame response sequence,
                # then initialize a new sequence incl. the first frame payload (excl. 1st and 2nd byte)
                if first_byte & self.FRAME_TYPE_MASK == self.FIRST_FRAME:
                    # create a new frame using information from previous iterations
                    if len(payload_concatenated) > 0:
                        new_frame = self.construct_new_frame(base_frame, payload_concatenated)
                        frame_list.append(new_frame.values.tolist())
                        frame_timestamp_list.append(frame_timestamp)

                    # reset and start on next frame
                    payload_concatenated = []
                    frame_timestamp = index
                    for byte in payload[self.first_frame_payload_start :]:
                        payload_concatenated.append(byte)

                # if consequtive frame, extend payload with payload excl. 1st byte
                if first_byte & self.FRAME_TYPE_MASK == self.CONSEQ_FRAME:
                    for byte in payload[1:]:
                        payload_concatenated.append(byte)

            df_raw_tp = pd.DataFrame(frame_list, columns=base_frame.index, index=frame_timestamp_list)
            df_raw_excl_tp = self.df_raw[~self.df_raw["ID"].isin(self.res_id_list)]
            df_raw_combined = df_raw_excl_tp.append(df_raw_tp).sort_index()
            df_raw_combined.index.name = "TimeStamp"
            return df_raw_combined

    def decode_multiframe_data(self, df_raw_combined, df_decoder):
        import pandas as pd

        df_phys_list = []
        # to process data with variable payload lengths for the same ID
        # it needs to be processed group-by-group based on the data length:
        df_grouped = df_raw_combined.groupby("DataLength")
        df_phys = pd.DataFrame()
        for length, group in df_grouped:
            df_phys_group = df_decoder.decode_frame(group)
            df_phys = df_phys.append(df_phys_group)

        df_phys = df_phys.sort_index()
        return df_phys

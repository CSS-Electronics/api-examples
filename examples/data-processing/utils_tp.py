class MultiFrameDecoder:
    """BETA class for handling transport protocol data. For each response ID, identify
    sequences of subsequent frames and combine the relevant parts of the data payloads
    into a single payload with the response ID as the ID. The original raw dataframe is
    then cleansed of the original response ID sequence frames. Instead, the new concatenated
    frames are inserted. Further, the class supports DBC decoding of the resulting modified raw data

    :param df_raw:                      dataframe of raw CAN data from the mdf_iter module
    :param res_id_list_hex:             list of transport protocol 'response CAN IDs' to process
    :param SINGLE_FRAME_MASK:           mask used in matching single frames
    :param FIRST_FRAME_MASK:            mask used in matching first frames
    :param CONSEQ_FRAME_MASK:           mask used in matching consequtive frames
    :param SINGLE_FRAME:                frame type reflecting a single frame response
    :param FIRST_FRAME:                 frame type reflecting the first frame in a multi frame response
    :param CONSEQ_FRAME:                frame type reflecting a consequtive frame in a multi frame response
    :param first_frame_payload_start:   the combined payload will start at this byte in the FIRST_FRAME
    :param bam_id_hex:                  used in e.g. J1939, this marks the initial BAM message ID in HEX
    """

    def __init__(self, df_raw, res_id_list_hex):
        self.df_raw = df_raw
        self.res_id_list_hex = res_id_list_hex
        self.res_id_list = [int(res_id, 16) for res_id in self.res_id_list_hex]

        return

    def construct_new_tp_frame(self, base_frame, payload_concatenated, can_id):
        new_frame = base_frame
        new_frame.at["DataBytes"] = payload_concatenated
        new_frame.at["DLC"] = 0
        new_frame.at["DataLength"] = len(payload_concatenated)

        if can_id:
            new_frame.at["ID"] = can_id

        return new_frame

    def combine_tp_frames(
        self,
        SINGLE_FRAME_MASK,
        FIRST_FRAME_MASK,
        CONSEQ_FRAME_MASK,
        SINGLE_FRAME,
        FIRST_FRAME,
        CONSEQ_FRAME,
        first_frame_payload_start,
        conseq_frame_payload_start,
        tp_type="",
        bam_id_hex="",
    ):
        import pandas as pd
        import sys

        df_raw_combined = pd.DataFrame()

        df_raw_excl_tp = self.df_raw[~self.df_raw["ID"].isin(self.res_id_list)]
        df_raw_combined = df_raw_excl_tp

        for channel, df_raw_channel in self.df_raw.groupby("BusChannel"):
            for res_id in self.res_id_list:
                # filter raw data for response ID and extract a 'base frame'
                if bam_id_hex == "":
                    bam_id = 0
                else:
                    bam_id = int(bam_id_hex, 16)

                df_raw_filter = df_raw_channel[df_raw_channel["ID"].isin([res_id, bam_id])]

                if df_raw_filter.empty:
                    continue

                base_frame = df_raw_filter.iloc[0]

                frame_list = []
                frame_timestamp_list = []
                payload_concatenated = []
                ff_length = 0xFFF
                can_id = None
                conseq_frame_prev = None

                # iterate through rows in filtered dataframe
                for index, row in df_raw_filter.iterrows():
                    payload = row["DataBytes"]
                    first_byte = payload[0]
                    row_id = row["ID"]

                    # if single frame, save frame directly (excl. 1st byte)
                    if first_byte & SINGLE_FRAME_MASK == SINGLE_FRAME:
                        new_frame = self.construct_new_tp_frame(base_frame, payload, row_id)
                        frame_list.append(new_frame.values.tolist())
                        frame_timestamp_list.append(index)

                    # if first frame, save info from prior multi frame response sequence,
                    # then initialize a new sequence incl. the first frame payload
                    elif ((first_byte & FIRST_FRAME_MASK == FIRST_FRAME) & (bam_id_hex == "")) or (bam_id == row_id):
                        # create a new frame using information from previous iterations
                        if len(payload_concatenated) >= ff_length:
                            new_frame = self.construct_new_tp_frame(base_frame, payload_concatenated, can_id)

                            frame_list.append(new_frame.values.tolist())
                            frame_timestamp_list.append(frame_timestamp)

                        # reset and start on next frame
                        payload_concatenated = []
                        conseq_frame_prev = None
                        frame_timestamp = index

                        # for J1939 BAM, extract PGN and convert to 29 bit CAN ID for use in baseframe
                        if bam_id_hex != "":
                            pgn_hex = "".join("{:02x}".format(x) for x in reversed(payload[5:8]))
                            pgn = int(pgn_hex, 16)
                            can_id = (6 << 26) | (pgn << 8) | 254

                        if tp_type == "uds" or tp_type == "nmea":
                            ff_length = (payload[0] & 0x0F) << 8 | payload[1]

                        for byte in payload[first_frame_payload_start:]:
                            payload_concatenated.append(byte)

                    # if consequtive frame, extend payload with payload excl. 1st byte
                    elif first_byte & CONSEQ_FRAME_MASK == CONSEQ_FRAME:
                        if (conseq_frame_prev == None) or ((first_byte - conseq_frame_prev) == 1):
                            conseq_frame_prev = first_byte
                            for byte in payload[conseq_frame_payload_start:]:
                                payload_concatenated.append(byte)

                df_raw_tp = pd.DataFrame(frame_list, columns=base_frame.index, index=frame_timestamp_list)
                df_raw_combined = df_raw_combined.append(df_raw_tp)

        df_raw_combined.index.name = "TimeStamp"
        df_raw_combined = df_raw_combined.sort_index()

        return df_raw_combined

    def decode_tp_data(self, df_raw_combined, df_decoder):
        import pandas as pd

        df_phys_list = []

        # to process data with variable payload lengths for the same ID
        # it needs to be processed group-by-group based on the data length:
        if df_raw_combined.empty:
            return df_raw_combined
        else:
            df_grouped = df_raw_combined.groupby("DataLength")
            df_phys = pd.DataFrame()
            for length, group in df_grouped:
                df_phys_group = df_decoder.decode_frame(group)
                df_phys = df_phys.append(df_phys_group)

            df_phys = df_phys.sort_index()
            return df_phys

    def combine_tp_frames_by_type(self, tp_type):
        conseq_frame_payload_start = 1
        bam_id_hex = ""

        if tp_type == "uds":
            SINGLE_FRAME_MASK = 0xF0
            FIRST_FRAME_MASK = 0xF0
            CONSEQ_FRAME_MASK = 0xF0
            SINGLE_FRAME = 0x00
            FIRST_FRAME = 0x10
            CONSEQ_FRAME = 0x20
            first_frame_payload_start = 2

        if tp_type == "j1939":
            SINGLE_FRAME_MASK = 0xFF
            FIRST_FRAME_MASK = 0xFF
            CONSEQ_FRAME_MASK = 0x00
            SINGLE_FRAME = 0xFF
            FIRST_FRAME = 0x20
            CONSEQ_FRAME = 0x00
            first_frame_payload_start = 8
            bam_id_hex = "0x1CECFF00"

        if tp_type == "nmea":
            SINGLE_FRAME_MASK = 0xFF
            FIRST_FRAME_MASK = 0x0F
            CONSEQ_FRAME_MASK = 0x00
            SINGLE_FRAME = 0xFF
            FIRST_FRAME = 0x00
            CONSEQ_FRAME = 0x00
            first_frame_payload_start = 2

        return self.combine_tp_frames(
            SINGLE_FRAME_MASK,
            FIRST_FRAME_MASK,
            CONSEQ_FRAME_MASK,
            SINGLE_FRAME,
            FIRST_FRAME,
            CONSEQ_FRAME,
            first_frame_payload_start,
            conseq_frame_payload_start,
            tp_type,
            bam_id_hex,
        )

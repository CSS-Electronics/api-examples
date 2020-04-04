"""
About: This is a utils module used in asammmdf_lite that contains functions from asammdf and canmatrix. The purpose
is to provide a light way to extract CAN signals for use cases where the full asammdf package is not ideal
"""

import numpy as np
import canmatrix.formats
from datetime import datetime, timezone

# below function is used for extracting a DBC message from a signal name:
def dbc_message(dbc, signal_name):
    return [f for f in dbc.frames if f.signal_by_name(signal_name) is not None][0]


# below function is used for extracting a DBC signal from a signal name:
def dbc_signal(dbc, signal_name):
    return [
        f.signal_by_name(signal_name)
        for f in dbc.frames
        if f.signal_by_name(signal_name) is not None
    ][0]


# below function is used for extracting a CAN signal's physical values
# credit: https://github.com/danielhrisca/asammdf/blob/development/asammdf/blocks/utils.py
def extract_can_signal(signal, payload):
    vals = payload

    big_endian = False if signal.is_little_endian else True
    signed = signal.is_signed

    start_bit = signal.get_startbit(bit_numbering=1)

    if big_endian:
        start_byte = start_bit // 8
        bit_count = signal.size

        pos = start_bit % 8 + 1

        over = bit_count % 8

        if pos >= over:
            bit_offset = (pos - over) % 8
        else:
            bit_offset = pos + 8 - over
    else:
        start_byte, bit_offset = divmod(start_bit, 8)

    bit_count = signal.size

    if big_endian:
        byte_pos = start_byte + 1
        start_pos = start_bit
        bits = bit_count

        while True:
            pos = start_pos % 8 + 1
            if pos < bits:
                byte_pos += 1
                bits -= pos
                start_pos = 7
            else:
                break

        if byte_pos > vals.shape[1] * 8:
            print(
                (
                    f'Could not extract signal "{signal.name}" with start '
                    f"bit {start_bit} and bit count {signal.size} "
                    f"from the payload with shape {vals.shape}"
                )
            )
    else:
        if start_bit + bit_count > vals.shape[1] * 8:
            print(
                f'Could not extract signal "{signal.name}" with start '
                f"bit {start_bit} and bit count {signal.size} "
                f"from the payload with shape {vals.shape}"
            )

    byte_size, r = divmod(bit_offset + bit_count, 8)
    if r:
        byte_size += 1

    if byte_size in (1, 2, 4, 8):
        extra_bytes = 0
    else:
        extra_bytes = 4 - (byte_size % 4)

    std_size = byte_size + extra_bytes

    # prepend or append extra bytes columns
    # to get a standard size number of bytes

    # print(signal.name, start_bit, bit_offset, start_byte, byte_size)

    if extra_bytes:
        if big_endian:

            vals = np.column_stack(
                [
                    vals[:, start_byte : start_byte + byte_size],
                    np.zeros(len(vals), dtype=f"<({extra_bytes},)u1"),
                ]
            )

            try:
                vals = vals.view(f">u{std_size}").ravel()
            except:
                vals = np.frombuffer(vals.tobytes(), dtype=f">u{std_size}")

            vals = vals >> (extra_bytes * 8 + bit_offset)
            vals &= (2 ** bit_count) - 1

        else:
            vals = np.column_stack(
                [
                    vals[:, start_byte : start_byte + byte_size],
                    np.zeros(len(vals), dtype=f"<({extra_bytes},)u1"),
                ]
            )
            try:
                vals = vals.view(f"<u{std_size}").ravel()
            except:
                vals = np.frombuffer(vals.tobytes(), dtype=f"<u{std_size}")

            vals = vals >> bit_offset
            vals &= (2 ** bit_count) - 1

    else:
        if big_endian:
            try:
                vals = (
                    vals[:, start_byte : start_byte + byte_size]
                    .view(f">u{std_size}")
                    .ravel()
                )
            except:
                vals = np.frombuffer(
                    vals[:, start_byte : start_byte + byte_size].tobytes(),
                    dtype=f">u{std_size}",
                )

            vals = vals >> bit_offset
            vals &= (2 ** bit_count) - 1
        else:
            try:
                vals = (
                    vals[:, start_byte : start_byte + byte_size]
                    .view(f"<u{std_size}")
                    .ravel()
                )
            except:
                vals = np.frombuffer(
                    vals[:, start_byte : start_byte + byte_size].tobytes(),
                    dtype=f"<u{std_size}",
                )

            vals = vals >> bit_offset
            vals &= (2 ** bit_count) - 1

    if signed:
        vals = as_non_byte_sized_signed_int(vals, bit_count)

    if (signal.factor, signal.offset) != (1, 0):
        vals = vals * float(signal.factor)
        vals += float(signal.offset)

    return vals


# this function is used by extract_can_signal
def as_non_byte_sized_signed_int(integer_array, bit_length):
    """
    The MDF spec allows values to be encoded as integers that aren't
    byte-sized. Numpy only knows how to do two's complement on byte-sized
    integers (i.e. int16, int32, int64, etc.), so we have to calculate two's
    complement ourselves in order to handle signed integers with unconventional
    lengths.
    Parameters
    ----------
    integer_array : np.array
        Array of integers to apply two's complement to
    bit_length : int
        Number of bits to sample from the array
    Returns
    -------
    integer_array : np.array
        signed integer array with non-byte-sized two's complement applied
    """

    if integer_array.flags.writeable:
        integer_array &= (1 << bit_length) - 1  # Zero out the unwanted bits
        truncated_integers = integer_array
    else:
        truncated_integers = integer_array & (
            (1 << bit_length) - 1
        )  # Zero out the unwanted bits
    return where(
        truncated_integers
        >> bit_length - 1,  # sign bit as a truth series (True when negative)
        (2 ** bit_length - truncated_integers)
        * -1,  # when negative, do two's complement
        truncated_integers,  # when positive, return the truncated int
    )


# get J1939 pgn from an extended CAN ID in hex format (string, e.g. 18F00E3D)
def get_pgn(can_id_hex):
    can_id_dec = int(can_id_hex, 16)
    pgn_dec = canmatrix.ArbitrationId(can_id_dec, extended=True).pgn
    pgn_hex = hex(pgn_dec)[2:].upper()

    return pgn_hex


# light can signal extractor for extracting a signal from a CSV file of raw data from the mdf2csv converter
def get_can_signal_lite(csv_log_file, dbc, signal, ignore_invalidation_bits=True):
    # specify csv column indices
    idx_time = 0
    idx_id = 2
    idx_data = 9

    # load dbc file and csv data (and check if J1939 type)
    db = canmatrix.formats.loadp_flat(dbc, "dbc")
    is_j1939 = db.contains_j1939

    raw_data = np.genfromtxt(csv_log_file, skip_header=1, delimiter=";", dtype="U20")

    # extract dbc details from signal
    message = dbc_message(db, signal)
    signal = dbc_signal(db, signal)
    can_id_dec = message.arbitration_id.id

    # if J1939 type, trigger based on PGNs instead of full CAN ID
    if is_j1939:
        can_id_dec = canmatrix.ArbitrationId(can_id_dec, extended=True).pgn

    can_id_hex = hex(can_id_dec)[2:].upper()

    timestamps = [
        datetime.fromtimestamp(float(row[idx_time][:]), timezone.utc)
        for row in raw_data
        if (is_j1939 != True and can_id_hex == row[idx_id])
        or (is_j1939 == True and can_id_hex == get_pgn(row[idx_id]))
    ]

    raw_data = [
        row[idx_data][:]
        for row in raw_data
        if (is_j1939 != True and can_id_hex == row[idx_id])
        or (is_j1939 == True and can_id_hex == get_pgn(row[idx_id]))
    ]

    raw_data = np.frombuffer(bytes.fromhex("".join(raw_data)), dtype="(8,)u1")
    phys_values = extract_can_signal(signal, raw_data)

    timestamps = np.array(timestamps)
    phys_values = np.array(phys_values)

    # filter out invalid data (raw value FF)
    if ignore_invalidation_bits:
        timestamps = timestamps[phys_values < signal.max]
        phys_values = phys_values[phys_values < signal.max]

    return timestamps, phys_values


# alternative DBC converter, using only the canmatrix library (for single payload conversion)
def get_can_signal_row(hexdata, dbc, signal):
    # hexdata: String of 8 CAN data bytes in the format "00FF2232F5C12289"
    db = canmatrix.formats.loadp_flat(dbc, "dbc")
    message = dbc_message(db, signal)
    payload = bytearray.fromhex(hexdata)
    decoded = message.decode(payload)
    return decoded[signal]

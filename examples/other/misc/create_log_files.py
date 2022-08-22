# this script can be used to generate log files for the CANedge e.g. for testing WiFi transfer

import os
import hashlib

# specify details of log files (device ID should match your test device)
device_id = "534F281B"
file_type = "MF4"
sessions = 120
splits = 50
size_bytes = 2 * 1024 * 1024

# function for creating files
def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65536)
            if len(buf) == 0:
                break
            digest.update(buf)
    return digest.hexdigest().upper()

# run loop to create log files in folders
dir_path = os.path.dirname(os.path.abspath(__file__))

for session_no in range(1, sessions + 1):
    for split_no in range(1, splits + 1):
        tmp_file_name = "{}.mf4".format(split_no)
        tmp_file_path = os.path.join(dir_path, tmp_file_name)

        with open(tmp_file_path, "w+") as f:
            f.seek(size_bytes - 1)
            f.write("\0")

        # Calculate digest of file
        digest = sha256_file(tmp_file_path)

        # create folder name
        folder = f'{session_no}'.zfill(8)

        # Check whether the specified path exists or not
        isExist = os.path.exists(folder)

        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(folder)

        # Create file name
        new_file_name = folder + "\\" + f"{split_no}".zfill(8) + ".MF4"
        # new_file_name = "{}_{:08}_{:08}-{}.mf4".format(device_id, session_no, split_no, digest)
        new_file_path = os.path.join(dir_path, new_file_name)

        os.rename(tmp_file_path, new_file_path)

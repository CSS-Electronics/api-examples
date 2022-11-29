import os
import hashlib


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65536)
            if len(buf) == 0:
                break
            digest.update(buf)
    return digest.hexdigest().upper()


device_id = "3851A144" #"1973B1D6"
file_type = "MF4"
sessions = 30
splits = 50
session_offset = 20000
size_bytes = 2 * 1024 * 1024

dir_path = os.path.dirname(os.path.abspath(__file__))

for session_no in range(session_offset, sessions + session_offset):
    for split_no in range(1, splits + 1):
        tmp_file_name = "{}.mf4".format(split_no)
        tmp_file_path = os.path.join(dir_path, tmp_file_name)

        with open(tmp_file_path, "w+") as f:
            f.seek(size_bytes - 1)
            f.write("\0")

        # Calculate digest of file
        digest = sha256_file(tmp_file_path)

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

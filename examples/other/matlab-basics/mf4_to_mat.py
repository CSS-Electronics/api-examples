from asammdf import MDF
import glob, sys, os
from pathlib import Path


# set variables
suffix_start = True  # include session start time in mat file names
raster = 1  # resamples output data to 1 second
mdf_extension = ".MF4"
input_folder = "LOG_datastore"
output_folder_mf4 = "LOG_mf4_decoded"
output_folder_mat = "LOG_mat_decoded"

# load MDF/DBC files from input folder
path = Path(__file__).parent.absolute()
path_in = Path(path, input_folder)
path_out_mf4 = Path(path, output_folder_mf4)
path_out_mat = Path(path, output_folder_mat)

dbc_files = {"CAN": [(dbc, 0) for dbc in list(path.rglob("dbc_files/*.dbc"))]}
logfiles = list(path_in.rglob("*" + mdf_extension))

print("Log file(s): ", logfiles, "\nDBC(s): ", dbc_files, "\n")

# export each logfile individually for use in e.g. datastore/tall array
for logfile in logfiles:
    # load MF4 log file and get the session start
    mdf = MDF(logfile)
    session_start = mdf.header.start_time.strftime("%Y%m%dT%H%M%S")

    # optionally use session start in output filename
    if suffix_start:
        mat_extension = f"-{session_start}.mat"
    else:
        mat_extension = ".mat"

    # specify output filenames
    filename = logfile.name
    filename_mat = str(filename).replace(".MF4", mat_extension)

    # re-use input path hierarchy for output
    rel_path = str(logfile).split(input_folder)[-1][1:].replace(filename, "")

    # set output paths using output hierarchy and filenames
    output_path_mf4 = Path(path_out_mf4, rel_path, filename)
    output_path_mat = Path(path_out_mat, rel_path, filename_mat)

    # dbc decode data
    mdf_scaled = mdf.extract_bus_logging(dbc_files)

    # EXPORT TO DBC DECODED MF4
    mdf_scaled.save(output_path_mf4, overwrite=True)

    # EXPORT TO DBC DECODED MAT (if output file does not exist)
    try:
        os.path.isfile(output_path_mat)
        print(f"MAT file already exists at {output_path_mat}")
    except:
        Path(output_path_mat).parent.mkdir(parents=True, exist_ok=True)

        mdf_scaled.export(
            "mat",
            filename=output_path_mat,
            time_from_zero=False,
            single_time_base=True,
            raster=raster,
            use_display_names=True,
            oned_as="column",
            keep_arrays=True,
        )

        print(f"Saving MAT file to {output_path_mat}")

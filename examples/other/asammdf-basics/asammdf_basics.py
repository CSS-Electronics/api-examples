"""
About: Load MDF log files & DBCs from an input folder and showcase various operations.
Note: Example assumes v6.4.4+ of asammdf
"""
from asammdf import MDF
import matplotlib.pyplot as plt
from datetime import timedelta
import glob, sys, os
from pathlib import Path


# set variables
mdf_extension = ".MF4"
input_folder = "input"
output_folder = "output"

# load MDF/DBC files from input folder
path = Path(__file__).parent.absolute()
path_in = Path(path, input_folder)
path_out = Path(path, output_folder)

dbc_files = {"CAN": [(dbc, 0) for dbc in list(path_in.glob("*" + ".DBC"))]}
logfiles = list(path_in.glob("*" + mdf_extension))

signals = ["EngineSpeed", "WheelBasedVehicleSpeed"]
print("Log file(s): ", logfiles, "\nDBC(s): ", dbc_files)

# concatenate MDF files from input folder and export as CSV incl. timestamps (localized time)
mdf = MDF.concatenate(logfiles)
mdf.save(Path(path_out, "conc"), overwrite=True)
mdf.export("csv", filename=Path(path_out, "conc"), time_as_date=True, time_from_zero=False)

# extract info from meta header - e.g. to get correct start time of a file
session_start = mdf.header.start_time
delta_seconds = mdf.select(["CAN_DataFrame.BusChannel"])[0].timestamps[0]
split_start = session_start + timedelta(seconds=delta_seconds)
split_start_str = split_start.strftime("%Y%m%d%H%M%S")

# filter an MDF
mdf_filter = mdf.filter(["CAN_DataFrame.ID", "CAN_DataFrame.DataBytes"])
mdf.save(Path(path_out, "filtered"), overwrite=True)

# DBC convert the unfiltered MDF + save & export resampled data
mdf_scaled = mdf.extract_bus_logging(dbc_files, ignore_invalid_signals=True)

mdf_scaled.save("scaled", overwrite=True)
mdf_scaled.export(
    "csv", filename=Path(path_out, "scaled"), time_as_date=True, time_from_zero=False, single_time_base=True, raster=0.5,
)

# extract a list of signals from a scaled MDF
mdf_scaled_signal_list = mdf_scaled.select(signals)

# extract a filtered MDF based on a signal list
mdf_scaled_signals = mdf_scaled.filter(signals)

# create pandas dataframe from the scaled MDF and e.g. add new signals
pd = mdf_scaled.to_dataframe(time_as_date=True)
pd["ratio"] = pd.loc[:, signals[0]] / pd.loc[:, signals[1]]
# pd_f = pd.loc["2020-01-13 13:00:35":"2020-01-13 13:59:56"]
# pd_f = pd_f[(pd_f[signals[0]] > 640)]
# print("\nFiltered pandas dataframe:\n", pd_f)

# trigger an action if a condition is satisfied
signal_stats = pd[signals[0]].agg(["count", "min", "max", "mean", "std"])
signal_diff = signal_stats["max"] - signal_stats["min"]
max_diff = 300

if signal_diff > max_diff:
    print(f"Filtered {signals[0]} max difference of {signal_diff} is above {max_diff}")
    pd.plot(y=signals[0])
    plt.savefig(Path(path_out, f"signal_{signals[0]}.png"))
    # do something, e.g. send a warning mail with a plot

"""
About: This module uses modified utils from asammdf/canmatrix to get CAN signals from a raw CSV file.
The format of the CSV is assumed to be from the CANedge mdf2csv converter (easily modified).
Useful for light data processing if e.g. asammdf is too extensive or not supported.
However, where possible we recommend to use the regular asammdf API.

Test: Last tested with canmatrix v0.9.1, asammdf dev (68cdeef) and MDF4 sample J1939 DBC
"""

import glob
import matplotlib.pyplot as plt
from csv_dbc_converter_utils import get_can_signal_lite, get_can_signal_row

# load CSV/DBC files from input folder
csv_log_file = glob.glob("input/*.csv")[0]
dbc = glob.glob("input/*.dbc")[0]
signals = ["EngineSpeed", "WheelBasedVehicleSpeed"]
print("CSV log file: ", csv_log_file, "\nDBC: ", dbc)

# extract timestamps (UTC) and physical values, e.g. for plotting:
fig, axs = plt.subplots(2)
for idx, signal in enumerate(signals):
    (timestamps, phys_values) = get_can_signal_lite(csv_log_file, dbc, signal)
    axs[idx].plot(timestamps, phys_values)
    axs[idx].set_title(signal)

plt.show()

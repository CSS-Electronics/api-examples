"""
About: This basic example shows how you can use the simple MDF4 converters in scripts
"""
import subprocess
from pathlib import Path

# get path of input/output folders relative to script location
path = Path(__file__).parent.absolute()

converter = str(Path(path, "mdf2csv.exe"))
path_in = str(Path(path, "input"))
path_out = str(Path(path, "output"))

# run converter
subprocess.run([converter, "-i", path_in, "-O", path_out])

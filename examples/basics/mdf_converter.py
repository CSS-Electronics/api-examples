"""
About: This basic example shows how you can use the simple MDF4 converters in scripts
"""
import subprocess

subprocess.run(["mdf2csv.exe", "-i", "input", "-O", "output"])

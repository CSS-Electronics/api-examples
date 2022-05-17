# CAN bus API examples (Python/MATLAB) | CANedge

This project includes Python and MATLAB examples of how to process MF4 log files with CAN/LIN data from your [CANedge](https://www.csselectronics.com/) data loggers. Most examples focus on the use of our Python API modules (`canedge_browser`, `mdf_iter`, `can_decoder`) for use with the CANedge log file formats (`MF4`, `MFC`, `MFE`, `MFM`). However, you'll also find other script examples incl. for the asammdf Python API, MATLAB, S3 and more.

---
## Features
```
For most use cases we recommend to start with the below examples:
- data-processing: List log files, load them and DBC decode the data (local, S3)

For some use cases the below examples may be useful:
- other/asammdf-basics: Load and concatenate MF4 logs, DBC decode them - and save as new MF4 files
- other/matlab-basics: Examples of how to load and use MF4/MAT CAN bus data 
- other/s3-basics: Examples of how to download, upload or list specific objects on your server
- other/s3-events: Using AWS Lambda or MinIO notifications (for event based data processing)
- other/misc: Example of automating the use of the MDF4 converters and misc tools

```

---

## Installation 

- Install Python 3.7 for Windows ([32 bit](https://www.python.org/ftp/python/3.7.9/python-3.7.9.exe)/[64 bit](https://www.python.org/ftp/python/3.7.9/python-3.7.9-amd64.exe)) or [Linux](https://www.python.org/downloads/release/python-379/) (_enable 'Add to PATH'_)
- Download this project as a zip via the green button and unzip it 
- Open the folder with the `requirements.txt` file and enter below in your [command prompt](https://www.youtube.com/watch?v=bgSSJQolR0E&t=47s):

##### Windows 
```
python -m venv env & env\Scripts\activate & pip install -r requirements.txt
python script_to_run.py
```

##### Linux 
```
python -m venv env && source env/bin/activate && pip install -r requirements.txt
python script_to_run.py
```

If you later need to re-activate the virtual environment, use `env\Scripts\activate`.

---

## Sample data (MDF4 & DBC)
The various folders include sample log files and DBC files. Once you've tested a script with the sample data, you can replace it with your own.

---

## Usage info
- Some example folders contain their own `README.md` files for extra information
- These example scripts are designed to be minimal and to help you get started - not for production
- Some S3 scripts use hardcoded credentials to ease testing - for production see e.g. [this guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)

---

## Which API modules to use?
There are many ways that you can work with the data from your CANedge devices. Most automation use cases involve fetching data from a specific device and time period - and DBC decoding this into a dataframe for further processing. Here, we recommend to look at the examples from the `data-processing/` folder. These examples use our custom modules designed for use with the CANedge: The [mdf_iter](https://pypi.org/project/mdf-iter/) (for loading MDF4 data), the [canedge_browser](https://github.com/CSS-Electronics/canedge_browser) (for fetching specific data locally or from S3) and the [can_decoder](https://github.com/CSS-Electronics/can_decoder) (for DBC decoding the data). In combination, these modules serve to support most use cases.

If you have needs that are not covered by these modules, you can check out the other examples using the asammdf API, the AWS/MinIO S3 API and our MDF4 converters.

If in doubt, [contact us](https://www.csselectronics.com/pages/contact-us) for sparring.

---
## About the CANedge

For details on installation and how to get started, see the documentation:
- [CANedge Docs](https://www.csselectronics.com/pages/can-bus-hardware-software-docs)  
- [CANedge1 Product Page](https://www.csselectronics.com/products/can-logger-sd-canedge1)  
- [CANedge2 Product Page](https://www.csselectronics.com/products/can-bus-data-logger-wifi-canedge2)  

---
## Contribution & support
Feature suggestions, pull requests or questions are welcome!

You can contact us at CSS Electronics below:  
- [www.csselectronics.com](https://www.csselectronics.com)  
- [Contact form](https://www.csselectronics.com/pages/contact-us)  

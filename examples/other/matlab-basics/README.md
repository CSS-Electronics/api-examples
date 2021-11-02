# Process MF4/MAT CAN bus data from the CANedge via MATLAB

Here you'll find examples for loading CANedge data in MATLAB in different ways.  We recommend that you check out our intro to using [MATLAB with CAN bus data](https://www.csselectronics.com/pages/matlab-mdf-mf4-can-bus) from the CANedge.

The scripts primarily focus on showcasing how you can load the log files from the CANedge in different ways.

Further, we also provide a script example letting you load `.mat` files instead of MF4 files, in case you prefer not to use the Vehicle Network Toolbox. To help automate the export of your MF4 to `.mat` we provide a plug & play script example.

---

## File overview

- The LOG folders contain data for the variuous script examples 
- `matlab_basics.m`: Load unfinalized and finalized MF4 log files via the VNT 
- `matlab_datastore.m`: Load several finalized MF4 files via datastores 
- `matlab_tall.m`: Load DBC decoded MF4 files into tall array (for big data)
- `matlab_mat.m`: Load DBC decoded `.mat` files into datastores and tall arrays 
- `mf4_to_mat.py`: DBC decodes MF4 log files and exports to `.mat` with suitable settings

---

## Installation/requirements

### Regarding MATLAB version
The MATLAB scripts are tested for release 2021b. The `matlab_basics.m`, `matlab_datastore.m` and `matlab_tallarray.m` assume you have the [Vehicle Network Toolbox](https://www.csselectronics.com/pages/matlab-mdf-mf4-can-bus) installed. Further, to load CANedge MF4 log files directly in MATLAB, the log files need to have been recorded with Firmware `01.04.01+`.

### Using the asammdf GUI to export MAT files
The `matlab_mat.m` example can be used with older versions of MATLAB and without using the Vehicle Network Toolbox. You can use the asammdf GUI to DBC decode and export your CANedge MF4 log files to the `.mat` format with settings as in the below picture. 

<img src="https://canlogger1000.csselectronics.com/img/asammdf-mat-output-settings.png" alt="asammdf GUI settings for MATLAB export of MF4" style="width:80%;">

### Using the asammdf Python API to export MAT files
Alternatively, you can use the asammdf Python API to automate this process via the `mf4_to_mat.py` script. See the general instructions for installing Python and the relevant `requirements.txt` in the `asammdf-basics/`. We generally recommend using the API to enable full automation of your workflow. 

---

## Documentation on using MF4 (MDF) in MATLAB's VNT

MATLAB provides a number of examples for how you can use MF4 data in MATLAB's Vehicle Network Toolbox in their [MDF overview](https://www.mathworks.com/help/vnt/mdf-files.html).

For details on working with tall arrays, see also MATLAB's [visualization of tall arrays](https://www.mathworks.com/help/matlab/import_export/tall-data-visualization.html) guide.
 
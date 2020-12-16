# CANedge data processing examples

Here you'll find examples for processing raw CANedge data to physical values - start with `process_data.py`.

## Installation

Download this folder, enter it, open your command prompt and run below:  
  ``pip install -r requirements.txt``

---

## File overview

- `LOG/`: Contains raw J1939 data samples
- `dbc_files/`: Contains demo/test DBC files for use in the examples
- `process_data.py`: List log files between dates, DBC decode them and perform various processing
- `process_tp_data.py`: Example of how multiframe data can be handled incl. DBC decoding (Transport Protocol)
- `utils.py`: Functions/classes used in the above scripts (note: Identical to utils.py from the dashboard-writer repo)
- `utils_tp.py`: Functions/classes used for Transport Protocol handling

---

### Regarding local disk vs S3
The examples load data from local disk by default. If you want to load data from your S3 server, modify `devices` to include a list of S3 device paths (e.g. `"my_bucket/device_id"`). In addition, you'll modify the `fs` initialization to include your S3 details as below:

fs = setup_fs(s3=True, key="access_key", secret="secret_key", endpoint="endpoint")

If you're using AWS S3, your endpoint would e.g. be `https://s3.us-east-2.amazonaws.com` (if your region is `us-east-2`). A MinIO S3 endpoint would e.g. be `http://192.168.0.1:9000`.

---

### Regarding Transport Protocol example
The example in `process_tp_data.py` should be seen as a very simplistic WIP TP implementation. It can be used as a starting point and will most likely need to be modified for individual use cases. We of course welcome any questions/feedback on this functionality.

The basic concept works as follows:

1. You specify a list of 'response IDs', which are the CAN IDs with multiframe responses  
2. The raw data is filtered by the response IDs and the payloads of these frames are combined  
3. The original response frames are then replaced by these re-constructed frames with payloads >8 bytes  
4. You can modify how the first/consequtive frames are interpreted (see the UDS and J1939 examples)  
5. The re-constructed data can be decoded using DBC files, optionally using multiplexing as in the sample UDS DBC files 

#### UDS example
For the basics on UDS, see the [Wikipedia article](https://en.wikipedia.org/wiki/Unified_Diagnostic_Services). The UDS example for device ID `17BD1DB7` shows real UDS response data from a Hyunda Kona EV. 

Below is a snippet of raw CAN data output before TP processing:

|TimeStamp|BusChannel                   |ID    |IDE                                          |DLC|DataLength|Dir  |EDL  |BRS  |DataBytes                              |
|---------|-----------------------------|------|---------------------------------------------|---|----------|-----|-----|-----|---------------------------------------|
|2020-12-15 14:15:00.316550+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[16, 62, 98, 1, 1, 255, 247, 231]      |
|2020-12-15 14:15:00.326550+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[33, 255, 100, 0, 0, 0, 0, 131]        |
|2020-12-15 14:15:00.336600+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[34, 0, 3, 13, 244, 10, 9, 9]          |
|2020-12-15 14:15:00.346600+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[35, 9, 9, 10, 0, 0, 10, 182]          |
|2020-12-15 14:15:00.356550+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[36, 35, 182, 50, 0, 0, 146, 0]        |
|2020-12-15 14:15:00.370950+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[37, 0, 1, 197, 0, 0, 4, 112]          |
|2020-12-15 14:15:00.376600+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[38, 0, 0, 0, 155, 0, 0, 1]            |
|2020-12-15 14:15:00.388250+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[39, 143, 0, 2, 157, 31, 9, 1]         |
|2020-12-15 14:15:00.397200+00:00|1                            |2028  |False                                        |8  |8         |False|False|False|[40, 101, 0, 0, 0, 0, 11, 184]         |
|2020-12-15 14:15:01.326600+00:00|1                            |1979  |False                                        |8  |8         |False|False|False|[16, 38, 98, 1, 0, 126, 80, 7]         |

After the above sequence is processed via the UDS TP script, it results in the below single frame:

```
2020-12-15 14:15:00.316550+00:00,1,2028,False,0,62,False,False,False,"[98, 1, 1, 255, 247, 231, 255, 100, 0, 0, 0, 0, 131, 0, 3, 13, 244, 10, 9, 9, 9, 9, 10, 0, 0, 10, 182, 35, 182, 50, 0, 0, 146, 0, 0, 1, 197, 0, 0, 4, 112, 0, 0, 0, 155, 0, 0, 1, 143, 0, 2, 157, 31, 9, 1, 101, 0, 0, 0, 0, 11, 184]"
```

Let's look at how this works in the script:

First, the script filters the data to show UDS IDs, e.g. `2028` (`0x7EC`). The script then identifies the 'First Frame' of an UDS sequence based on the 1st byte value (`16`). The script then extracts payload data from the First Frame (starting from the 3rd byte) and all 'Consequtive Frames' (starting from the 2nd byte). The result is a new frame that receives the timestamp of the First Frame and the full data payload. The script 'finalizes' a frame once a new 'First Frame' is identified.

Note that the 1st data byte of this new frame is the UDS Response Service ID (SID), while the 2nd to 3rd bytes reflect the UDS Data Identifier (DID). A UDS DBC file can thus use extended multiplexing to decode signals, utilizing the SID and DID as sequential multiplexors to distinguish between different UDS service modes and DIDs. See the UDS DBC file examples for a starting point on how this can be constructed.

The script merges the reconstructed UDS frames into the original data (removing the original entries of the response ID). The result is a new raw dataframe that can be processed as you would normally do (using a suitable DBC file). The above example has an associated DBC file, `tp_uds_hyundai_soc.dbc`, which lets you extract e.g. State of Charge.

-----
### Pending improvements
- Improve the UDS/J1939 scripts to alternatively trigger the creation of a new combined frame once the payload exceeds the data length 

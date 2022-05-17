# CANedge data processing examples

Here you'll find examples for processing raw CANedge data to physical values.

---

## File overview

- `LOG/`: Contains raw data samples (J1939, NMEA 2000, UDS)
- `dbc_files/`: Contains demo/test DBC files for use in the examples
- `process_data.py`: List log files between dates, DBC decode them and perform various processing
- `process_tp_data.py`: Example of how multiframe data can be handled incl. DBC decoding (Transport Protocol)
- `utils.py`: Functions/classes used in the above scripts (note: Identical to utils.py from the dashboard-writer repo)

---

## Installation

See the README in the above folder.

---

### Regarding local disk vs S3
The examples load data from local disk by default. If you want to load data from your S3 server, modify `devices` to include a list of S3 device paths (e.g. `"my_bucket/device_id"`). In addition, you'll modify the `fs` initialization to include your S3 details as below:

```
fs = setup_fs(s3=True, key="access_key", secret="secret_key", endpoint="endpoint")
```

If you're using AWS S3, your endpoint would e.g. be `https://s3.us-east-2.amazonaws.com` (if your region is `us-east-2`). A MinIO S3 endpoint would e.g. be `http://192.168.0.1:9000`.

---
### Regarding encrypted log files
If you need to handle encrypted log files, you can provide a passwords dictionary object with similar structure as the `passwords.json` file used in the CANedge MF4 converters. The object can be provided e.g. as below (or via environmental variables):

```
pw = {"default": "password"} 			# hardcoded  
pw = json.load(open("passwords.json"))	# from local JSON file 
```

---

### Regarding Transport Protocol example
The example in `process_tp_data.py` should be seen as a simplistic TP implementation. It can be used as a starting point and will most likely need to be modified for individual use cases. We of course welcome any questions/feedback on this functionality.

The basic concept works as follows:

1. You specify the "type" of transport protocol: UDS (`uds`), J1939 (`j1939`) or NMEA 2000 Fast Packets (`nmea`)
2. The raw data is filtered by the protocol-specific 'TP response IDs' and the payloads of these frames are combined  
3. The original response frames are then replaced by these re-constructed frames with payloads >8 bytes  
4. The re-constructed data can be decoded using DBC files, optionally using multiplexing as in the sample UDS DBC files 

#### Implementing TP processing in other scripts 
To use the Transport Protocol functionality in other scripts, you need to make minor modifications:

1. Ensure that you import the `MultiFrameDecoder` class from `utils.py` 
2. Specify the type via the `tp_type` variable e.g. to `j1939` 
3. After you've extract the normal raw dataframe, parse it to the `tp.combine_tp_frames` function as below

See below example:

```
tp_type = "j1939"
df_raw, device_id = proc.get_raw_data(log_file)
tp = MultiFrameDecoder(tp_type)
df_raw = tp.combine_tp_frames(df_raw)
```


#### UDS example
For UDS basics see our [UDS tutorial](https://www.csselectronics.com/pages/uds-protocol-tutorial-unified-diagnostic-services). The UDS example for device `17BD1DB7` shows UDS response data from a Hyundai Kona EV. 

Below is a snippet of raw CAN data output before TP processing:

```
TimeStamp,BusChannel,ID,IDE,DLC,DataLength,Dir,EDL,BRS,DataBytes
2020-12-15 14:15:00.316550+00:00,1,2028,False,8,8,False,False,False,"[16, 62, 98, 1, 1, 255, 247, 231]"
2020-12-15 14:15:00.326550+00:00,1,2028,False,8,8,False,False,False,"[33, 255, 100, 0, 0, 0, 0, 131]"
2020-12-15 14:15:00.336600+00:00,1,2028,False,8,8,False,False,False,"[34, 0, 3, 13, 244, 10, 9, 9]"
2020-12-15 14:15:00.346600+00:00,1,2028,False,8,8,False,False,False,"[35, 9, 9, 10, 0, 0, 10, 182]"
2020-12-15 14:15:00.356550+00:00,1,2028,False,8,8,False,False,False,"[36, 35, 182, 50, 0, 0, 146, 0]"
2020-12-15 14:15:00.370950+00:00,1,2028,False,8,8,False,False,False,"[37, 0, 1, 197, 0, 0, 4, 112]"
2020-12-15 14:15:00.376600+00:00,1,2028,False,8,8,False,False,False,"[38, 0, 0, 0, 155, 0, 0, 1]"
2020-12-15 14:15:00.388250+00:00,1,2028,False,8,8,False,False,False,"[39, 143, 0, 2, 157, 31, 9, 1]"
2020-12-15 14:15:00.397200+00:00,1,2028,False,8,8,False,False,False,"[40, 101, 0, 0, 0, 0, 11, 184]"
2020-12-15 14:15:01.326600+00:00,1,1979,False,8,8,False,False,False,"[16, 38, 98, 1, 0, 126, 80, 7]"
```

After the above sequence is processed via the UDS TP script, it results in the below single frame:

```
2020-12-15 14:15:00.316550+00:00,1,2028,False,0,62,False,False,False,"[98, 1, 1, 255, 247, 231, 255, 100, 0, 0, 0, 0, 131, 0, 3, 13, 244, 10, 9, 9, 9, 9, 10, 0, 0, 10, 182, 35, 182, 50, 0, 0, 146, 0, 0, 1, 197, 0, 0, 4, 112, 0, 0, 0, 155, 0, 0, 1, 143, 0, 2, 157, 31, 9, 1, 101, 0, 0, 0, 0, 11, 184]"
```

Let's look at how this works in the script:

First, the script filters the data to show only the filtered UDS response IDs, in this case `2028` (`0x7EC`). The script then iterates through the data line-by-line until it encounters the 'First Frame' of an UDS sequence (identified based on the 1st byte value, `16`). Next, the script extracts bytes 2-7 from the First Frame and concatenates these with bytes 1-7 of the 'Consequtive Frames'. The script 'finalizes' the constructed frame once a new 'First Frame' is encountered.

The first 3 bytes of this new frame should be interpreted as follows:
- Byte 0: This is the UDS Response Service ID (SID)
- Bytes 1-2: This is the UDS Data Identifier (DID)

Often you'll see references to UDS extended PIDs, e.g. `0x220101`. Here, `0x22` is the request service (with `0x62` being the corresponding response service). The `0x0101` is the DID. 

A UDS DBC file can use extended multiplexing to decode UDS signals, utilizing the SID and DID as sequential multiplexors to distinguish between different UDS service modes and DIDs. See the UDS DBC file examples for a starting point on how this can be constructed.

The script merges the reconstructed UDS frames into the original data (removing the original entries of the response ID). The result is a new raw dataframe that can be processed as you would normally do (using a suitable DBC file). The above example has an associated DBC file, `tp_uds.dbc`, which lets you extract e.g. State of Charge.

The script also contains an example of a proprietary UDS-style request/response from a Nissan Leaf 2019 for State of Charge (SoC) and battery pack temperatures.

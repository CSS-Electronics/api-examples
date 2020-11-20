# CANedge data processing examples

Here you'll find examples for processing raw CANedge data to physical values - start with `process_data.py`.

## Installation 

Download this folder, enter it, open your command prompt and run below:  
  ``pip install -r requirements.txt``
  
--- 

## File overview

- `LOG/`: Contains raw J1939 data samples
- `dbc_files/`: Contains demo/test DBC files for use in the examples
- `process_data.py`: List log files between dates, DBC decode them and export/analyze the data 
- `create_custom_signal.py`: Example of creating a custom signal based on two existing signals 
- `decode_multiframe_data.py`: Example of how multiframe data can be handled incl. DBC decoding (Transport Protocol)
- `utils.py`: Functions/classes used in the above scripts 

---

### Regarding Transport Protocol example 
The example in `decode_multiframe_data.py` should be seen as very basic and not as a formal TP implementation. It can be used as a starting point and will most likely need to be modified for individual use cases. We of course welcome any questions/feedback on this functionality. 

The basic concept works as follows:

1. You specify a list of 'response IDs', which are the CAN IDs with multiframe responses  
2. The raw data is filtered by the response IDs and the payloads of these frames are combined  
3. The original response frames are then replaced by these re-constructed frames with payloads >8 bytes  
4. You can modify how the first/consequtive frames are interpreted (we include NMEA2000, UDS and J1939 examples)  
5. The re-constructed data can be decoded using DBC files, optionally using multiplexing
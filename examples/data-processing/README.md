# CANedge data processing examples
Here you'll find examples for processing raw CANedge data to physical values. 

## File overview

- `LOG/`: Contains raw J1939 data samples
- `CSS-Electronics-SAE-J1939-DEMO.dbc`: A demo J1939 DBC for decoding J1939 data 
- `process_data.py`: List log files between dates, DBC decode them and export/analyze the data 
- `create_custom_signal.py`: Create a custom signal based on two existing signals 
- `decode_multiframe_data.py`: Example of basic Transport Protocol frame handling incl. DBC decoding
- `utils.py`: This contains a number of functions used in the above scripts 

---

## Regarding Transport Protocol example 
The example in `decode_multiframe_data.py` should be seen as very basic and not as a formal TP implementation. It can be used as a starting point and will most likely need to be modified for individual use cases. We of course welcome any questions/feedback on this functionality. 


% This script uses the MATLAB Vehicle Network Toolbox to load
% finalized & sorted MF4 files from the CANedge (Firmware 01.04.01+)
% See also below links:
% - https://se.mathworks.com/help/vnt/ug/reading-data-from-mdf-files.html
% - https://se.mathworks.com/help/vnt/ug/using-mdf-files-via-mdf-datastore.html
% - https://se.mathworks.com/help/vnt/ug/decoding-can-data-from-mdf-files.html

% load a sorted & finalized MF4 file (via the mdf2finalized converter)
m = mdf('00000001_fin.MF4')

% extract 5 rows into a timetable
rawTimeTable = read(m, 1, m.ChannelNames{1}, 1, 5)

% extract data using DBC file (note: MATLAB sees the CANedge data as CAN FD
% regardless of whether it's Classical CAN or CAN FD)
db = canDatabase('my_dbc.dbc');
msgTimetable    = canFDMessageTimetable(rawTimeTable, db)

% create a signal time table for a specific CAN message and create a plot
signalTimetable1 = canSignalTimetable(msgTimetable, "Message1")
plot(signalTimetable1.Time, signalTimetable1.Signal1)

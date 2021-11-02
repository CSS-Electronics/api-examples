clear, clc, close all

% load database
canDB = canDatabase("dbc_files/canmod-gps.dbc");

% create datastore of finalized MF4 files from local disk
mds = mdfDatastore("LOG_datastore/3BA199E2/00000164", "IncludeSubfolders", true);

% same principle can be used to load MF4 files from mounted S3 drive
% mds = mdfDatastore("Z:\3BA199E2","IncludeSubfolders", true);

% preview datastore
preview(mds);

% ------------------------------------------------------------------------
% if datastore fits into memory you can simply read all data into timetable 
rawTimeTable = readall(mds);
msgTimetable = canFDMessageTimetable(rawTimeTable, canDB);
msgSpeed = canSignalTimetable(msgTimetable, "gnss_speed");
subplot(2, 1, 1);
plot(msgSpeed.Time, msgSpeed.Speed)
ylabel("Speed (m/s) via readall")
 
% ------------------------------------------------------------------------
% if datastore is larger than memory, use in-memory chunks
mds.ReadSize = seconds(300);
msgSpeed = [];
i = 1;

while hasdata(mds)
    % read a chunk, decode it and extract data to separate table
    rawTimeTable = read(mds);
    msgTimeTable = canFDMessageTimetable(rawTimeTable, canDB);
    msgSpeedChunk = canSignalTimetable(msgTimetable, "gnss_speed");    
    msgSpeed = vertcat(msgSpeed,msgSpeedChunk);
    fprintf("\nreading chunk %i",i)
    i = i + 1;
end

subplot(2, 1, 2);
plot(msgSpeed.Time, msgSpeed.Speed)
ylabel("Speed (m/s) via chunks")
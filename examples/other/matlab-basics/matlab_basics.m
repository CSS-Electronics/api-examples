clear, clc, close all

% set index of CAN channel (MATLAB finalization: 8 | mdf2finalized: 1) 
can_idx = 8;

% ------------------------------------------------------------------------
% finalize & load MF4 "in place" (overwrites original file)
% m = mdf(mdfFinalize("LOG/11111111/00000012/00000001.MF4"));

% finalize & load MF4 "out of place" (makes a copy of original file)
try
    finalizedPath2 = mdfFinalize("LOG/11111111/00000012/00000001.MF4", "LOG/11111111/00000012/00000001_fin.MF4");
    m = mdf(finalizedPath2);
catch ME
    disp(ME.message)
end

% load an MF4 which has already been 'finalized' via MATLAB or mdf2finalized
m = mdf("LOG/11111111/00000012/00000001_fin.MF4");

% extract CAN data into timetable
rawTimeTable = read(m,can_idx,m.ChannelNames{can_idx});



% ------------------------------------------------------------------------
% decode CAN data using DBC, use absolute date & time and extract a specific message
canDB = canDatabase('dbc_files/canmod-gps.dbc');
msgTimetableGPS = canFDMessageTimetable(rawTimeTable, canDB);
msgTimetableGPS.Time = msgTimetableGPS.Time + m.InitialTimestamp;
msgSpeed = canSignalTimetable(msgTimetableGPS, "gnss_speed");

% decode J1939 data (first convert data to 'Classical' CAN by removing EDL)
rawTimeTable = removevars(rawTimeTable, "CAN_DataFrame_EDL");

canDB = canDatabase('dbc_files/CSS-Electronics-SAE-J1939-DEMO.dbc');
msgTimetableJ1939 = j1939ParameterGroupTimetable(rawTimeTable, canDB);
msgTimetableJ1939.Time = msgTimetableJ1939.Time + m.InitialTimestamp;
msgEEC1 = j1939SignalTimetable(msgTimetableJ1939, "ParameterGroups","EEC1");

% plot select decoded signals 
ax1 = subplot(2, 1, 1);
plot(msgSpeed.Time, msgSpeed.Speed)
ylabel("Speed (m/s)")
ax2 = subplot(2, 1, 2);
plot(msgEEC1.Time, msgEEC1.EngineSpeed)
ylabel("Engine Speed (rpm)")
linkaxes([ax1,ax2],'x');

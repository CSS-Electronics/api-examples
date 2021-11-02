clear, clc, close all

% create datastore of finalized MF4 files from local disk or mounted S3 drive
mds = mdfDatastore("LOG_mf4_decoded/3BA199E2/00000164", "IncludeSubfolders", true);

% preview datastore
preview(mds);

% view datastore CAN message groups and turn relevant group into tall array
mds.ChannelGroups;

mds.SelectedChannelGroupNumber = 3;
tt1 = tall(mds);

mds.SelectedChannelGroupNumber = 9;
tt2 = tall(mds);

% create deferred calculations
meanPositionAccuracy = mean(tt1.PositionAccuracy);
medianPositionAccuracy = median(tt1.PositionAccuracy);
maxTime = max(tt1.Time);

% use gather to force computation of deferred calculations
[Latitude, Longitude, meanPositionAccuracy, medianPositionAccuracy, maxTime] = gather(tt1.Latitude, tt1.Longitude, meanPositionAccuracy, medianPositionAccuracy, maxTime);


fprintf("\nThe mean [median] position accuracy is %s [%s] meters\n", num2str(meanPositionAccuracy), num2str(medianPositionAccuracy));

% plot variables and note that how plot() supports tall arrays directly
subplot(4,1,1)
geoplot(Latitude,Longitude,'-')
title("GNSS position");

ax1 = subplot(4,1,2);
plot(tt1.Time,tt1.PositionAccuracy)
title("Position accuracy (m)");

ax2 = subplot(4,1,3);
plot(tt2.Time,tt2.AccelerationX)
title("Acceleration X (incl. invalid)");

ax3 = subplot(4,1,4);
plot(tt2.Time,tt2.AccelerationX ./ logical(tt2.ImuValid))
title("Acceleration X (excl. invalid)");

linkaxes([ax1,ax2,ax3],'x');
xlim([0 maxTime]);


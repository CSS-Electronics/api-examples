clear, clc, close all

% load decoded MAT 7.3 files from folder into datastore and tall array
ds = fileDatastore("LOG_mat_decoded/3BA199E2/00000164",'ReadFcn', @(x)struct2table(load(x)), 'UniformRead', true, 'IncludeSubfolders', true);
tt = tall(ds);

% create deferred calculations
meanSpeed = mean(tt.Speed);

% use gather to force computation of deferred calculations
[meanSpeed] = gather(meanSpeed);


plot(tt.timestamps,tt.Speed)
fprintf("\nThe average speed is %s m/s\n",num2str(meanSpeed))

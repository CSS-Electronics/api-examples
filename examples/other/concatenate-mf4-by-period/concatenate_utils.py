def extract_mdf_start_stop_time(mdf):
    from datetime import timedelta

    # function to identify start/stop timestamp of concatenated log file
    session_start = mdf.header.start_time
    df_raw_asam = mdf.to_dataframe()    
    delta_seconds_start = df_raw_asam.index[0]
    delta_seconds_stop = df_raw_asam.index[-1]
    mdf_start = session_start + timedelta(seconds=delta_seconds_start)
    mdf_stop = session_start + timedelta(seconds=delta_seconds_stop)

    return mdf_start, mdf_stop

def hour_rounder(t):
    from datetime import timedelta

    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))


def finalize_log_files(log_files, path_output_temp, path_mdf2finalized):
    import subprocess
    from pathlib import Path
    import glob
    
    for log_file in log_files:
            
        path_output_file_temp_name = Path(*log_file.parts[1:3])
        
        try:
            Path(path_output_temp / path_output_file_temp_name).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(e)
            
        subprocess.run([path_mdf2finalized, "-i", log_file, "-O", path_output_temp / path_output_file_temp_name,])
    log_files = list(path_output_temp.glob('**/*.MF4'))
    return log_files
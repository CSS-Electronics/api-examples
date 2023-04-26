def extract_mdf_start_stop_time(mdf):
    from datetime import timedelta

    # function to identify start/stop timestamp of concatenated log file
    df_raw_asam = mdf.to_dataframe(time_as_date=True)    
    mdf_start = df_raw_asam.index[0]
    mdf_stop = df_raw_asam.index[-1]
   
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
    import shutil

    path_output_temp_finalized = path_output_temp.parent / "temp_finalized"

    for log_file in log_files:
        path_output_file_temp_name = Path(*log_file.parts[-3:][0:2])

        # create repository for finalized files
        try:
            Path(path_output_temp_finalized / path_output_file_temp_name).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(e)

        # create repository for unfinalized files
        try:
            Path(path_output_temp / path_output_file_temp_name).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(e)

        # copy log file to local disk first
        shutil.copy(log_file, path_output_temp / path_output_file_temp_name)

        # finalize the copied file
        subprocess.run(
            [
                path_mdf2finalized,
                "-i",
                path_output_temp / path_output_file_temp_name / log_file.name,
                "-O",
                path_output_temp_finalized / path_output_file_temp_name,
            ]
        )

    log_files = list(path_output_temp_finalized.glob("**/*.MF4"))

    return log_files

def setup_fs_s3():
    """Helper function to setup a remote S3 filesystem connection.
    """
    import s3fs

    fs = s3fs.S3FileSystem(
        key="<key>",
        secret="<secret>",
        client_kwargs={
            "endpoint_url": "<endpoint>",
            # "verify": "path\\to\\public_certificate.crt",  # for TLS enabled MinIO servers
        },
    )

    return fs


def setup_fs():
    """Helper function to setup the file system.
    """
    from pathlib import Path
    import canedge_browser

    base_path = Path(__file__).parent
	
	# Setup path to local folder structure, as if copied from a CANedge SD.
    # Assumes the folder is placed in same directory as this file
    fs = canedge_browser.LocalFileSystem(base_path=base_path)

    return fs


def custom_sig(df, signal1, signal2, function, new_signal):
    """Helper function for calculating a new signal based on two signals and a function.
    Returns a dataframe with the new signal name and physical values
    """
    import pandas as pd

    try:
        s1 = df[df["Signal"] == signal1]["Physical Value"].rename(signal1)
        s2 = df[df["Signal"] == signal2]["Physical Value"].rename(signal2)

        df_new_sig = pd.merge_ordered(
            s1, s2, on="TimeStamp", fill_method="ffill",
        ).set_index("TimeStamp")

        df_new_sig = (
            df_new_sig.apply(lambda x: function(x[0], x[1]), axis=1)
            .dropna()
            .rename("Physical Value")
            .to_frame()
        )

        df_new_sig["Signal"] = new_signal

        return df_new_sig

    except:
        print(f"Warning: Custom signal {new_signal} not created\n")
        return pd.DataFrame()

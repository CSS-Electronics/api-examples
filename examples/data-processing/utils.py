def setup_fs_s3():
    """Helper function to setup a remote S3 filesystem connection.
    """
    import s3fs

    fs = s3fs.S3FileSystem(
        key="<key>", secret="<secret>", client_kwargs={"endpoint_url": "<endpoint>"},
    )

    return fs


def setup_fs():
    """Helper function to setup the local file system.
    """
    from fsspec.implementations.local import LocalFileSystem
    from pathlib import Path

    # Setup path to local folder structure, as if copied from a CANedge SD.
    # Assumes the folder is placed in same directory as this file
    fs = LocalFileSystem()

    return fs

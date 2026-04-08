# Upload SD Log Files to S3

Manually upload CANedge log files (MF4/MFC) from a local folder to S3, including S3 device metadata.

Tested with FW 01.09.XX.

## Usage

1. Place a device config JSON (e.g. `config-01.09.json`) next to the script
2. Add your SD card `LOG/` folder next to the script
4. Run: `python upload_sd_to_s3.py`

# Event Records Processor
This Python script processes event records from an SQLite database containing GPS coordinates and timestamps. It converts the GPS coordinates into human-readable addresses and converts timestamps into Eastern Standard Time (EST). The processed data is saved in batches as CSV files.

## Features
- Reads data from an SQLite database table.
- Converts latitude and longitude to addresses using the Nominatim geolocation service.
- Converts timestamps from milliseconds since epoch to EST format.
- Splits the data into batches and saves each batch as a CSV file.
- Automatically resumes from the last batch if the script was previously interrupted.

## Prerequisites
Ensure you have the following installed:
- Python 3.x
- Required Python packages:
  ```bash
  pip install pandas geopy pytz
  ```

## Usage
Run the script from the command line using the following format:
```bash
python script_name.py -f <path_to_db_file> -t <table_name> -o <output_prefix>
```

### Arguments:
- `-f, --db_file`: Path to the SQLite database file (required).
- `-t, --table_name`: Name of the database table to query (required).
- `-o, --output_prefix`: Prefix for the output CSV files (default: `EVENT_RECORDS_with_est_batch_`).

### Example:
```bash
python script_name.py -f my_database.db -t event_records -o gps_batch_
```

This command processes the `event_records` table in `my_database.db` and saves output files as `gps_batch_<batch_number>.csv`.

## Output
- CSV files containing the processed data with columns:
  - `TIMESTAMP_MS`: Original timestamp in milliseconds.
  - `LATITUDE`: Latitude of the event location.
  - `LONGITUDE`: Longitude of the event location.
  - `Address`: Human-readable address based on the GPS coordinates.
  - `Timestamp_EST`: Timestamp converted to EST.

## Notes
- The script uses the [Nominatim](https://nominatim.org/) geocoding service. Ensure you comply with their usage policy to avoid rate limiting.
- A delay (`0.3 seconds`) is added between geolocation requests to prevent being blocked.

## Error Handling
- If an address lookup fails, the script will return "Address not found" and continue processing.
- If the database connection or query fails, check the database file path and table name.

## License
This script is provided as-is for educational and practical use.


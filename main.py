'''
Get Address and Timestamp from GPS Coordinates in db file.
'''


import argparse
from datetime import datetime, timedelta
import math
import os
import sqlite3
import time
import pytz
import pandas as pd
from geopy.geocoders import Nominatim

# Set up argument parsing.
parser = argparse.ArgumentParser(description='Process event records from an SQLite database.')
parser.add_argument('-f', '--db_file', required=True, help='Path to the SQLite database file')
parser.add_argument('-t', '--table_name', required=True, help='Name of the table to query')
parser.add_argument('-o', '--output_prefix', default='EVENT_RECORDS_with_est_batch_', help='Prefix for output CSV files')
args = parser.parse_args()

# Load the SQLite database.
db_file_path = args.db_file
conn = sqlite3.connect(db_file_path)

# Query the data from the specified table.
query = f'''
SELECT 
    TIMESTAMP_MS,
    LATITUDE,
    LONGITUDE
FROM 
    {args.table_name}
ORDER BY 
    TIMESTAMP_MS DESC;
'''
data = pd.read_sql_query(query, conn)

# Close the database connection.
conn.close()

# Instantiate the Nominatim geolocator.
geolocator = Nominatim(user_agent='event_records_processor')

# Function to convert latitude and longitude to address.
def get_address(lat, lon):
    try:
        location = geolocator.reverse(f'{lat}, {lon}').raw
        address = location.get('display_name', 'Address not found')
        name = location.get('name', '')
        place_type = location.get('type', '')
        return f'{name}, {address} ({place_type})' if name and place_type else address
    except Exception as e:
        print(f'Error fetching address for {lat}, {lon}: {e}')
        return 'Address not found'

# Function to convert TIMESTAMP_MS from milliseconds to EST.
def convert_to_est(timestamp_ms):
    utc_time = datetime(1970, 1, 1) + timedelta(milliseconds=timestamp_ms)
    est = pytz.timezone('US/Eastern')
    utc = pytz.timezone('UTC')
    utc_time = utc.localize(utc_time)
    est_time = utc_time.astimezone(est)
    return est_time.strftime('%Y-%m-%d %H:%M:%S %Z')

# Determine the last processed batch
output_directory = '.'  # Assuming the script is running in the same directory where files are saved
existing_files = [f for f in os.listdir(output_directory) if f.startswith(args.output_prefix) and f.endswith('.csv')]
existing_batches = sorted([int(f.split('_')[-1].split('.')[0]) for f in existing_files])

if existing_batches:
    last_processed_batch = max(existing_batches)
    print(f'Last processed batch: {last_processed_batch}. Resuming from batch {last_processed_batch + 1}.')
    start_batch = last_processed_batch + 1
else:
    print('No existing batch files found. Starting from batch 1.')
    start_batch = 1

# Split the data into batches
batch_size = 100
total_batches = math.ceil(len(data) / batch_size)

for batch_number in range(start_batch - 1, total_batches):
    start_index = batch_number * batch_size
    end_index = min(start_index + batch_size, len(data))
    
    addresses = []
    timestamps_est = []

    for index, row in data.iloc[start_index:end_index].iterrows():
        address = get_address(row['LATITUDE'], row['LONGITUDE'])
        timestamp_est = convert_to_est(row['TIMESTAMP_MS'])
        addresses.append(address)
        timestamps_est.append(timestamp_est)

        # Print progress
        print(f'Batch {batch_number + 1}, Processed row {index + 1}: Address: {address}, Timestamp_EST: {timestamp_est}')

        time.sleep(.3)  # Wait for 1 second between requests to handle rate limiting

    # Add the processed data to the batch DataFrame
    batch_data = data.iloc[start_index:end_index].copy()
    batch_data['Address'] = addresses
    batch_data['Timestamp_EST'] = timestamps_est

    # Save each batch to a separate CSV file
    output_file_path = f'{args.output_prefix}{batch_number + 1}.csv'
    batch_data.to_csv(output_file_path, index=False)

    print(f'Batch {batch_number + 1} complete. Data saved to {output_file_path}')

print('Processing complete for all batches.')
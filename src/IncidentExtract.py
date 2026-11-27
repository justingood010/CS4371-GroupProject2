import os
import json
import time
import hashlib
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the token securely
APP_TOKEN = os.getenv("CHICAGO_APP_TOKEN")
if not APP_TOKEN:
    raise ValueError("API Token not found! Make sure you have a .env file with CHICAGO_APP_TOKEN.")

# Setup folders
CACHE_DIR = Path("data/cache")
DATA_RAW_DIR = Path("data/raw")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
headers = {"X-App-Token": APP_TOKEN}

start_date = "2022-01-01T00:00:00"
end_date = "2025-01-01T00:00:00"
where_clause = f"date >= '{start_date}' AND date < '{end_date}'"

limit = 20000
offset = 0
all_data = []

print("Starting download (with caching)...")

while True:
    # Setup parameters
    params = {
        "$where": where_clause,
        "$limit": limit,
        "$offset": offset,
        "$order": "date ASC"
    }

    # --- CACHING LOGIC ---
    # Calculate the range for a unique, limit-independent filename
    start_row = offset
    end_row = offset + limit
    cache_filename = CACHE_DIR / f"cache_{start_row}_{end_row}.json"

    if cache_filename.exists():
        print(f"Cache hit! Loading data from {cache_filename.name}")
        with open(cache_filename, "r") as f:
            cache_payload = json.load(f)
            data = cache_payload["data"]
    else:
        print(f"Cache miss. Fetching rows {start_row} to {end_row} from API...")
        
        # Retry logic for 503 errors or network blips
        max_retries = 4
        data = []
        
        for attempt in range(max_retries):
            try:
                # Wait at least 1 second between all calls
                time.sleep(1.1) 
                response = requests.get(URL, headers=headers, params=params)
                
                # If the server is busy, wait and retry
                if response.status_code == 503:
                    print(f"Server busy (503). Retrying {attempt + 1}/{max_retries} in 5 seconds...")
                    time.sleep(5) 
                    continue
                
                # Raise an exception for other bad status codes (like 429 or 500)
                response.raise_for_status()
                data = response.json()
                break # Success! Break out of the retry loop
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    print("Max retries reached for this chunk. Stopping script.")
                time.sleep(5)

        # Save the response to cache
        cache_payload = {
            "received_at": time.time(),
            "data": data
        }
        with open(cache_filename, "w") as f:
            json.dump(cache_payload, f)

    # If no data is returned, we have reached the end of the dataset
    if not data:
        print("Finished retrieving all records.")
        break

    # Add the newly fetched/cached data to our master list
    all_data.extend(data)
    print(f"Retrieved {len(all_data)} total records so far...")

    # Increment offset for the next loop
    offset += limit

# Convert to DataFrame and Save
if all_data:
    df = pd.DataFrame(all_data)
    print(f"Total records downloaded: {len(df)}")
    
    # Save to the raw data folder
    out_path = DATA_RAW_DIR / "chicago_incidents_2022_2024.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Data saved to {out_path}")
else:
    print("No data found.")
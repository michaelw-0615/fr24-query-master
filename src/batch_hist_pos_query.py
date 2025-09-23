'''
This script provides an example of batch-querying historical flight positions.
We look for detailed flight position data (including lon/lat, height, speed, ETA)
for all American Airlines (AA/AAL) flights between its hub airports on Jan 1, 2024.
Data records are queried on an hourly basis, and are saved to one .json file.
Please read the comments and the README file carefully before changing the script according to your needs.
ChatGPT 5 is used for assistance during the creation of this script.
'''

import requests
import time
import json
import os
from datetime import datetime, timedelta
from typing import List
API_TOKEN = os.environ.get("FR24_API_TOKEN", "YOUR_API_TOKEN") 
# Replace the second param with the actual API access token. I have intentionally removed my token for security reasons, since this is an open repo.

def fetch_historic_flight_positions(api_token: str, start_date: datetime, end_date: datetime, interval_seconds: int = 15 * 60, **filters) -> List[dict]:
    """
    Fetches historical flight positions over a date range, provided by FR24.

    Parameters:
        api_token (str): Your Flightradar24 API token.
        start_date (datetime): The start date and time.
        end_date (datetime): The end date and time.
        interval_seconds (int): Time interval between data points in seconds. Default is 15 minutes (900 seconds).
        **filters: Endpoint filters like bounds, flights, callsigns, registrations, limit etc.

    Returns:
        List[dict]: A list of flight position data.
    """
    api_url = 'https://fr24api.flightradar24.com/api/historic/flight-positions/full'
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {api_token}'
    }

    # Generate list of timestamps
    timestamps = []
    current_time = start_date
    delta = timedelta(seconds=interval_seconds)

    while current_time <= end_date:
        timestamps.append(int(current_time.timestamp()))
        current_time += delta

    all_data = []

    for ts in timestamps:
        params = {'timestamp': ts}
        params.update(filters)  # Add additional filters if provided

        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json().get('data', [])
            all_data.extend(data)
            print(f"Timestamp {ts}: Retrieved {len(data)} records")

        elif response.status_code == 429:
            print(f"Rate limit reached. Sleeping for {response.headers.get('Retry-After', 60)} seconds.")
            time.sleep(int(response.headers.get('Retry-After', 60)))

            # Retry the request after sleeping
            response = requests.get(api_url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json().get('data', [])
                all_data.extend(data)
                print(f"Timestamp {ts}: Retrieved {len(data)} records after retry")
            else:
                print(f"Error {response.status_code} for timestamp {ts} after retry")
        else:
            print(f"Error {response.status_code} for timestamp {ts}")

    return all_data

# ---- Slice route list into chunks with size 15 ----
def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def save_json(data, path="flight_data.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[OK] Saved {len(data)} records → {path}")

# ---- Optional: de-duplication across batches ----
def dedupe_by_id_ts(records):
    seen, out = set(), []
    for r in records:
        key = (r.get("fr24_id"), r.get("timestamp"))
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out

# ---- ----
def fetch_positions_with_route_batches(
    api_token: str,
    start_date: datetime,
    end_date: datetime,
    routes: list[str],
    batch_size: int = 15,
    interval_seconds: int = 60*60,
    out_json: str = "outputs/batch_flight_data.json",
    dedupe: bool = True,
    sleep_between_batches: float = 0.3,
    **other_filters
):
    '''
    Batch process using fetch_historic_flight_positions function provided by FR24
    Input include a string list of routes (e.g. ["JFK-LAX", "LAX-JFK", ...]),
    output to a single JSON file.
    '''
    all_records = []
    total_batches = (len(routes) + batch_size - 1) // batch_size

    for bi, chunk in enumerate(chunked(routes, batch_size), start=1):
        routes_param = ",".join(chunk)  
        print(f"[INFO] Batch {bi}/{total_batches} — routes={routes_param}")

        # Call the fetch function with current batch of routes
        records = fetch_historic_flight_positions(
            api_token=api_token,
            start_date=start_date,
            end_date=end_date,
            interval_seconds=interval_seconds,
            routes=routes_param,
            **other_filters
        )
        print(f"[INFO] Batch {bi}: +{len(records)} records")
        all_records.extend(records)
        time.sleep(sleep_between_batches)

    if dedupe:
        before = len(all_records)
        all_records = dedupe_by_id_ts(all_records)
        print(f"[INFO] Dedup: {before} → {len(all_records)}")

    save_json(all_records, out_json)
    return all_records

# ================== Batch run ==================
if __name__ == "__main__":

    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date   = datetime(2024, 1, 1, 23, 59, 59)

    # All routes considered (airport pairs w/o direct flights--e.g. JFK-PHL--are manually excluded)
    ROUTES = [
        "JFK-DFW","JFK-DCA","JFK-ORD","JFK-LAX","JFK-MIA","JFK-CLT","JFK-PHX",
        "LGA-DFW","LGA-DCA","LGA-ORD","LGA-MIA","LGA-CLT","JFK-LAX","JFK-PHX",
        "DCA-JFK","DCA-LGA","DCA-ORD","DCA-MIA","DCA-CLT","DCA-DFW","DCA-LAX","DCA-PHX",
        "CLT-DCA","CLT-PHL","CLT-JFK","CLT-LGA","CLT-ORD","CLT-MIA","CLT-DFW","CLT-LAX","CLT-PHX",
        "ORD-JFK","ORD-LGA","ORD-PHL","ORD-DCA","ORD-MIA","ORD-CLT","ORD-DFW","ORD-LAX","ORD-PHX",
        "MIA-JFK","MIA-LGA","MIA-PHL","MIA-DCA","MIA-CLT","MIA-ORD","MIA-DFW","MIA-LAX","MIA-PHX",
        "DFW-JFK","DFW-LGA","DFW-PHL","DFW-DCA","DFW-CLT","DFW-ORD","DFW-MIA","DFW-LAX","DFW-PHX",
        "LAX-JFK","LAX-LGA","LAX-PHL","LAX-DCA","LAX-CLT","LAX-ORD","LAX-MIA","LAX-DFW","LAX-PHX",
        "PHX-JFK","PHX-LGA","PHX-PHL","PHX-DCA","PHX-CLT","PHX-ORD","PHX-MIA","PHX-DFW","PHX-LAX",
        "PHL-CLT","PHL-ORD","PHL-MIA","PHL-DFW","PHL-LAX","PHL-PHX"
    ]
        

    # All other filters
    other_filters = {
        "limit": 1000,
        "bounds": '50.000,24.500,-125.000,-66.000', # Continental US
        "interval_seconds": 30 * 60,  # one query per 30 minutes
        "operating_as": 'AAL',
        "painted_as": 'AAL'
    }

    fetch_positions_with_route_batches(
        api_token=API_TOKEN,
        start_date=start_date,
        end_date=end_date,
        routes=ROUTES,
        batch_size=15,                 # Default max batch size regulated by FR24 server
        out_json="outputs/batch_flight_data.json",   # merge & save to this file
        dedupe=False,                   # No de-duplication for now
        **other_filters
    )

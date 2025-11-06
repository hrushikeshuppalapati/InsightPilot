import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# last 12 months
end = datetime.utcnow()
start = end - timedelta(days=365)

COLUMNS = [
    "unique_key","created_date","closed_date","agency",
    "complaint_type","descriptor","borough","city",
    "incident_zip","status","open_data_channel_type",
    "latitude","longitude"
]

LIMIT = 50000

def fetch_chunk(offset):
    params = {
        "$select": ",".join(COLUMNS),
        "$where": f"created_date between '{start:%Y-%m-%dT%H:%M:%S}' and '{end:%Y-%m-%dT%H:%M:%S}'",
        "$limit": LIMIT,
        "$offset": offset
    }
    url = f"{BASE}?{urlencode(params)}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def main():
    os.makedirs("data", exist_ok=True)
    dfs = []
    for i in range(3):   # 3 chunks ~150K rows
        df = fetch_chunk(i * LIMIT)
        if df.empty:
            break
        dfs.append(df)
        print(f"Fetched {len(df)} rows (chunk {i+1})")

    data = pd.concat(dfs, ignore_index=True)
    for col in ["created_date","closed_date"]:
        data[col] = pd.to_datetime(data[col], errors="coerce")
    data["resolution_hours"] = (
        (data["closed_date"] - data["created_date"]).dt.total_seconds() / 3600
    )

    data.to_parquet("data/nyc311.parquet", index=False)
    print(f"Saved {len(data)} rows → data/nyc311.parquet")

if __name__ == "__main__":
    main()

import pandas as pd
import requests
import time
import os
from tqdm import tqdm  # pip install tqdm

# --- CONFIGURATION ---
# The script will read THIS file and overwrite it with new data
FILE_PATH = "facilities_metareviews_rag_ready.parquet"

# Naver API Keys (Set these in your terminal or hardcode them here)
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

# --- GEOCODING FUNCTION ---
def get_naver_coordinates(address):
    """
    Returns (lat, lon) from Naver Maps.
    """
    if not address or not isinstance(address, str):
        return None, None

    headers = {
        "X-NCP-APIGW-API-KEY-ID": NAVER_CLIENT_ID,
        "X-NCP-APIGW-API-KEY": NAVER_CLIENT_SECRET,
    }
    url = "https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode"
    
    try:
        response = requests.get(url, headers=headers, params={"query": address}, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("addresses"):
                x = float(data["addresses"][0]["x"]) # Longitude
                y = float(data["addresses"][0]["y"]) # Latitude
                return y, x
    except Exception:
        pass # Fail silently to keep loop moving
        
    return None, None

# --- MAIN SCRIPT ---
def main():
    # 1. Check if file exists
    if not os.path.exists(FILE_PATH):
        print(f"❌ Error: File '{FILE_PATH}' not found in current folder.")
        return

    # 2. Load Data
    print(f"📂 Loading {FILE_PATH}...")
    df = pd.read_parquet(FILE_PATH)
    total_rows = len(df)
    print(f"   Loaded {total_rows} rows.")

    # 3. Initialize Columns if missing
    if "lat" not in df.columns: df["lat"] = None
    if "lon" not in df.columns: df["lon"] = None

    # 4. Filter for missing rows (Resumable Logic)
    # We only want to process rows where lat is NaN or Null
    missing_mask = df["lat"].isna() | df["lat"].isnull()
    rows_to_process = df[missing_mask].index
    
    print(f"   Rows needing Geocoding: {len(rows_to_process)}")
    print("🚀 Starting Geocoding... (Press Ctrl+C to stop safely)")

    processed_count = 0
    save_interval = 100 # Save every 100 rows to prevent data loss

    try:
        # Iterate only over the rows that need it
        for idx in tqdm(rows_to_process):
            address = df.at[idx, "address"]
            
            # Skip empty addresses
            if not address:
                continue

            # Call API
            lat, lon = get_naver_coordinates(address)

            if lat:
                df.at[idx, "lat"] = lat
                df.at[idx, "lon"] = lon
            
            processed_count += 1
            time.sleep(0.05) # Rate limit (20 req/s max)

            # Periodic Save (Overwrites the file)
            if processed_count % save_interval == 0:
                df.to_parquet(FILE_PATH)
                
    except KeyboardInterrupt:
        print("\n🛑 Process stopped by user. Saving progress...")

    # 5. Final Save
    df.to_parquet(FILE_PATH)
    print(f"\n✅ Done! Updated file saved to: {FILE_PATH}")
    print(f"   Processed this run: {processed_count}")
    print(f"   Remaining missing: {len(df[df['lat'].isna()])}")

if __name__ == "__main__":
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        print("❌ Error: NAVER_CLIENT_ID and NAVER_CLIENT_SECRET must be set.")
    else:
        main()

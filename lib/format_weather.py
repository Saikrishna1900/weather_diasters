import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import date
import json

HOME = os.path.expanduser("~")
RAW_ROOT = f"{HOME}/datalake/raw/weather/"
FORMATTED_ROOT = f"{HOME}/datalake/formatted/weather/"

def format_weather_data(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    raw_path = f"{RAW_ROOT}{current_day}/weather.json"
    out_dir = f"{FORMATTED_ROOT}{current_day}/"
    os.makedirs(out_dir, exist_ok=True)

    print(f"✅ Reading JSON: {raw_path}")
    if not os.path.exists(raw_path):
        print(f"❌ File not found: {raw_path}")
        return

    with open(raw_path, "r") as f:
        data = json.load(f)

    if "daily" not in data:
        print("❌ Key 'daily' not found in JSON. Exiting.")
        return

    df = pd.DataFrame(data["daily"])
    print(f"✅ Original DataFrame shape: {df.shape}")
    print(f"✅ Columns: {df.columns.tolist()}")

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"]).astype(str)  # 👈 convert timestamp to string

    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{out_dir}weather.parquet")

    print(f"✅ Cleaned & saved Parquet to: {out_dir}weather.parquet")

# Run manually
if __name__ == "__main__":
    format_weather_data()

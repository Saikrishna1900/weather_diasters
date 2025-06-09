import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import date

HOME = os.path.expanduser("~")
RAW_ROOT = f"{HOME}/datalake/raw/disasters/"
FORMATTED_ROOT = f"{HOME}/datalake/formatted/disasters/"

def format_disaster_data(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    raw_path = f"{RAW_ROOT}{current_day}/disasters.csv"
    out_dir = f"{FORMATTED_ROOT}{current_day}/"
    os.makedirs(out_dir, exist_ok=True)

    print(f"✅ Reading CSV: {raw_path}")
    df = pd.read_csv(raw_path)

    print(f"✅ Raw DataFrame shape: {df.shape}")
    df.columns = df.columns.str.replace('"', '').str.strip()

    df_long = df.melt(id_vars=["Month"], var_name="Year", value_name="Value")
    df_long["Year"] = df_long["Year"].astype(str).str.strip()
    df_long["Value"] = pd.to_numeric(df_long["Value"], errors='coerce')

    print(f"✅ Transformed shape: {df_long.shape}")
    table = pa.Table.from_pandas(df_long)
    pq.write_table(table, f"{out_dir}disasters.parquet")
    print(f"✅ Saved: {out_dir}disasters.parquet")

if __name__ == "__main__":
    format_disaster_data()

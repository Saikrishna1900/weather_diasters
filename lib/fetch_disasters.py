import os
import requests
from datetime import date

# Set root output path
HOME = os.path.expanduser("~")
ROOT = f"{HOME}/datalake/raw/disasters/"

def fetch_disaster_csv(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    output_path = f"{ROOT}{current_day}/"
    print("✅ Output Path:", output_path)
    os.makedirs(output_path, exist_ok=True)

    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hurricanes.csv"

    try:
        response = requests.get(url)
        response.raise_for_status()
        print("✅ API Status Code:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("❌ Error during file download:", e)
        return

    file_path = output_path + "disasters.csv"
    with open(file_path, "wb") as f:
        f.write(response.content)

    print("✅ File written to:", file_path)

# ✅ Allow script to run directly
if __name__ == "__main__":
    fetch_disaster_csv()

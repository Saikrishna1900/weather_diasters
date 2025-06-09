import os
import requests
import json
from datetime import date

# Set output root path
HOME = os.path.expanduser('~')
ROOT = f"{HOME}/datalake/raw/weather/"

def fetch_weather_data(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    output_path = f"{ROOT}{current_day}/"
    print("✅ Output Path:", output_path)
    os.makedirs(output_path, exist_ok=True)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "start_date": "2024-01-01",
        "end_date": "2024-01-10",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Europe/Berlin"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        print("✅ API Status Code:", response.status_code)
        print("✅ Response Keys:", response.json().keys())
    except requests.exceptions.RequestException as e:
        print("❌ Error during API request:", e)
        return

    file_path = output_path + "weather.json"
    with open(file_path, "w") as f:
        json.dump(response.json(), f, indent=4)

    print("✅ File written to:", file_path)

# ✅ Allow standalone execution
if __name__ == "__main__":
    fetch_weather_data()

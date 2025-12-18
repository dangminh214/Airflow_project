import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_api_data(**context):
    API_URL = os.environ.get("API")

    OUTPUT_DIR = "data"
    assert API_URL != None

    KEY = os.environ.get("KEY")
    assert KEY != None

    execution_date = context["ds"]  # YYYY-MM-DD

    response = requests.get(f"{API_URL}?{KEY}", timeout=30)
    response.raise_for_status()

    data = response.json()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/fetch_data_{execution_date}.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Data saved to {output_file}")
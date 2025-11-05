import os
import requests

api_key = os.getenv("OPENAQ_API_KEY")
headers = {"X-API-Key": api_key}
resp = requests.get("https://api.openaq.org/v3/countries", headers=headers)

for c in resp.json().get("results", []):
    print(f"{c['code']} - {c['name']}")

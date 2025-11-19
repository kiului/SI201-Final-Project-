
import requests
import json

# Example endpoint: GDP per capita (NY.GDP.PCAP.CD) for all countries
BASE_URL = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD"

params = {
    "format": "json",
    "per_page": 50,   # how many results per page (max 50)
    "page": 1
}

response = requests.get(BASE_URL, params=params)

print("Status code:", response.status_code)

if response.status_code != 200:
    print("Request failed:")
    print(response.text)
else:
    data = response.json()
    print(json.dumps(data, indent=4))

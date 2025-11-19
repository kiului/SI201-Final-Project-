import requests
import json

API_KEY = "e92c3942dfe584525e4535af0db5bd23"
  # put your key here
city = "London"                 # change to whatever
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "q": city,
    "appid": API_KEY,
    "units": "metric"   # remove if you want Kelvin
}

response = requests.get(BASE_URL, params=params)

# check if it worked
if response.status_code != 200:
    print("Request failed:", response.status_code, response.text)
else:
    data = response.json()
    print(json.dumps(data, indent=4))
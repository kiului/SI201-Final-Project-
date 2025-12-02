import requests
import json

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"   # ← put your real key here

BASE_URL = "https://api.openaq.org/v3/locations/2178"  # example location ID

headers = {
    "X-API-Key": API_KEY
}

# Optional: add params depending on what you want
params = {
    # leave empty or add things like limit/page if the endpoint supports it
}

response = requests.get(BASE_URL, headers=headers, params=params)

print("Status code:", response.status_code)

if response.status_code != 200:
    print("Request failed:")
    print(response.text)
else:
    data = response.json()
    print(json.dumps(data, indent=4))


def fetch_openaq_data(city=None, country=None, limit=100):
    """
    Fetches latest air quality measurements from OpenAQ API v3.

    Args:
        city (str): Optional city name filter.
        country (str): Optional 2-letter country code filter.
        limit (int): Number of results to return.

    Returns:
        pd.DataFrame: A DataFrame of measurements.
    """

    params = {
        "limit": limit
    }

    if city:
        params["city"] = city
    if country:
        params["country"] = country

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if "results" not in data or len(data["results"]) == 0:
            print("No results found.")
            return pd.DataFrame()

        # Flatten the measurements into a DataFrame
        records = []
        for item in data["results"]:
            location = item.get("location")
            city = item.get("city")
            country = item.get("country")

            for measure in item.get("measurements", []):
                records.append({
                    "location": location,
                    "city": city,
                    "country": country,
                    "parameter": measure.get("parameter"),
                    "value": measure.get("value"),
                    "unit": measure.get("unit"),
                    "lastUpdated": measure.get("lastUpdated")
                })

        return pd.DataFrame(records)

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    df = fetch_openaq_data(city="Los Angeles", country="US", limit=50)
    print(df.head())

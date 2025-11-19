import requests
url = "https://search.worldbank.org/api/v3/wds?format=json&qterm=wind%20turbine&fl=docdt,count"
response = requests.get(url)
data = response.json()
print(data)
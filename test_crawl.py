# Shopee API Example
import requests

url = "https://open.shopee.com/developer-guide/12"
headers = {
    "Content-Type": "application/json"
}

response = requests.get(url)
data = response.json()
print(data)
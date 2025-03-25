import requests

API_KEY = "18cb89441355cd5468d49bf725b17940"
SPORT_KEY = "basketball_nba"  # NBA için spor anahtarı
REGIONS = "us"  # ABD'deki bahis oranları için
MARKETS = "h2h"
BASE_URL = "https://api.the-odds-api.com/v4/sports/"

# NBA etkinliklerini çek
response = requests.get(
    f"{BASE_URL}{SPORT_KEY}/odds",
    params={
        "apiKey": API_KEY,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": "decimal"
    }
)

if response.status_code == 200:
    events = response.json()
    print("NBA etkinlikleri başarıyla alındı!")
    for event in events:
        print(f"Event ID: {event['id']}, Event Name: {event['home_team']} vs {event['away_team']}")
else:
    print(f"Hata: {response.status_code}, {response.text}")

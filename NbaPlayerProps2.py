import requests
import datetime
import pytz
import pandas as pd

def get_nba_player_props(api_key):
    # Pandas ayarları (Tüm satır ve sütunları göstermek için)
    pd.options.display.max_rows = None
    pd.options.display.max_columns = None
    pd.set_option('display.width', None)  # Ekranda genişlik sınırlamasını kaldır

    # API bilgileri
    SPORT = "basketball_nba"
    REGION = "us"  # ABD bahis oranlarını çek
    MARKETS = ("player_points,player_assists,player_rebounds"
               ",player_threes,player_blocks,player_steals,player_blocks_steals,player_turnovers"
               ",player_points_rebounds_assists,player_points_rebounds,player_points_assists"
               ",player_rebounds_assists,player_field_goals,player_frees_made,player_frees_attempts")

    # Saat dilimleri
    est = pytz.timezone('America/New_York')  # ABD Doğu Saati (EST)
    utc = pytz.utc  # Koordinatlı Evrensel Zaman (UTC)

    # Şu anki zaman
    now_utc = datetime.datetime.now(utc)

    # EST saatine çevrilen başlangıç ve bitiş zamanı
    start_time_est = now_utc.astimezone(est).replace(hour=8, minute=0, second=0, microsecond=0)
    end_time_est = start_time_est + datetime.timedelta(hours=19)

    # API URL'si
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds/?regions={REGION}&markets=h2h&apiKey={api_key}"

    # API'den veri çek
    response = requests.get(url)
    data = response.json()

    # Maçları filtreleme
    match_ids = []
    for game in data:
        try:
            game_time_utc = datetime.datetime.fromisoformat(game["commence_time"][:-1])
            game_time_est = game_time_utc.replace(tzinfo=utc).astimezone(est)

            if start_time_est <= game_time_est <= end_time_est:
                match_ids.append({
                    "id": game["id"],
                    "home_team": game["home_team"],
                    "away_team": game["away_team"],
                    "game_time_est": game_time_est.strftime('%Y-%m-%d %H:%M:%S')
                })
        except Exception as e:
            print(f"Hata oluştu: {e}")

    # Oyuncu bahislerini almak için istek atma
    player_props_list = []
    for match_id in match_ids:
        url_props = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{match_id['id']}/odds/?regions={REGION}&markets={MARKETS}&apiKey={api_key}"

        response = requests.get(url_props)

        if response.status_code == 200:
            player_props = response.json()

            for bookmaker in player_props.get("bookmakers", []):
                if bookmaker["title"] == "FanDuel":
                    for market in bookmaker.get("markets", []):
                        for outcome in market.get("outcomes", []):
                            player_props_list.append({
                                "match_id": match_id["id"],
                                "home_team": match_id["home_team"],
                                "away_team": match_id["away_team"],
                                "game_time_est": match_id["game_time_est"],
                                "bookmaker": bookmaker["title"],
                                "player": outcome["description"],
                                "prop_type": market["key"],
                                "line": outcome.get("point", None),
                                "odds": outcome["price"],
                            })
        else:
            print(f"İstek başarısız oldu, HTTP {response.status_code}")

    # DataFrame oluştur
    df_props = pd.DataFrame(player_props_list)

    # Yalnızca istediğin sütunları seç
    df_filtered = df_props[['home_team', 'away_team', 'game_time_est', 'player', 'prop_type', 'line']]
    df_unique = df_filtered.drop_duplicates()

    # prop_type isimlerini değiştirme
    prop_type_rename_map = {
        "player_points": "PTS",
        "player_assists": "AST",
        "player_rebounds": "REB",
        "player_threes": "FG3M",
        "player_blocks": "BLK",
        "player_steals": "STL",
        "player_blocks_steals": "STL_BLK",
        "player_turnovers": "TOV",
        "player_points_rebounds_assists": "PTS_AST_REB",
        "player_points_rebounds": "PTS_REB",
        "player_points_assists": "PTS_AST",
        "player_rebounds_assists": "AST_REB",
        "player_field_goals": "FGM",
        "player_frees_made": "FTM",
        "player_frees_attempts": "FTA"
    }

    df_unique.loc[:, 'prop_type'] = df_unique['prop_type'].replace(prop_type_rename_map)

    # Pivot işlemi
    df_pivot = df_unique.pivot(index=['home_team', 'away_team', 'game_time_est', 'player'],
                               columns='prop_type',
                               values='line').reset_index()

    # Sütun adını değiştirme
    df_pivot = df_pivot.rename(columns={'player': 'PLAYER_NAME'})

    return df_pivot

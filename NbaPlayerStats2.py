from nba_api.stats.endpoints import playergamelog, commonallplayers, scoreboardv2
import pandas as pd
import time
import datetime

def fetch_nba_stats(target_players):
    """
    Sadece target_players listesinde bulunan oyuncuların istatistiklerini çeker.

    :param target_players: Liste halinde oyuncu isimleri
    :param season: Hangi sezonun istatistikleri çekilecek
    :return: Pandas DataFrame
    """
    # NBA'deki tüm aktif oyuncuları al
    season = "2024-25"
    all_players = commonallplayers.CommonAllPlayers(is_only_current_season=1)
    players = all_players.get_data_frames()[0]

    # Oyuncu ID ve İsimlerini al
    active_players = players[['PERSON_ID', 'DISPLAY_FIRST_LAST']]

    # Sadece bahis oranlarında gelen oyuncuların ID'lerini al
    filtered_players = active_players[active_players['DISPLAY_FIRST_LAST'].isin(target_players)]

    all_stats = []

    for index, row in filtered_players.iterrows():
        player_id = row['PERSON_ID']
        player_name = row['DISPLAY_FIRST_LAST']

        try:
            # Oyuncunun maç istatistiklerini çek
            game_log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            player_stats = game_log.get_data_frames()[0]

            # Oyuncu adını ekle
            player_stats['PLAYER_NAME'] = player_name

            # Sonuç listesine ekle
            all_stats.append(player_stats)

            print(f"{player_name}'ın maç istatistikleri alındı.")
        except Exception as e:
            print(f"{player_name} için hata: {e}")

        # Hız sınırına takılmamak için bekleme
        time.sleep(0.5)

    # Tüm istatistikleri birleştir
    if all_stats:
        all_stats_df = pd.concat(all_stats, ignore_index=True)
        all_stats_df = all_stats_df.dropna(axis=0, how='all')
        return all_stats_df
    else:
        return pd.DataFrame()  # Eğer hiç oyuncu verisi çekilemezse boş DataFrame döndür


def fetch_last_24h_nba_stats():
    """
    Son 24 saat içinde oynanan NBA maçlarındaki tüm oyuncuların istatistiklerini çeker.

    :return: Pandas DataFrame (Son 24 saatte oynanan maçlardaki oyuncuların istatistikleri)
    """
    # Şu anki tarih ve saat
    now = datetime.datetime.utcnow()
    one_day_ago = now - datetime.timedelta(days=1)

    # YYYYMMDD formatında tarih almak için
    date_str = one_day_ago.strftime("%Y-%m-%d")

    # Son 24 saatte oynanan maçları al
    try:
        scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str)
        games = scoreboard.get_data_frames()[0]
    except Exception as e:
        print(f"Maç listesi alınırken hata oluştu: {e}")
        return pd.DataFrame()  # Eğer veri alınamazsa boş DataFrame döndür

    game_ids = games["GAME_ID"].unique().tolist()

    if not game_ids:
        print(f"{date_str} tarihinde oynanan maç bulunamadı.")
        return pd.DataFrame()

    all_stats = []

    for game_id in game_ids:
        try:
            # Her maç için oyuncu istatistiklerini al
            game_stats = playergamelog.PlayerGameLog(season="2024-25")
            stats_df = game_stats.get_data_frames()[0]

            # Maç kimliği ve tarihi ekleyelim
            stats_df["GAME_ID"] = game_id
            stats_df["game_day"] = one_day_ago.strftime("%Y-%m-%d")

            all_stats.append(stats_df)

            print(f"Maç ID: {game_id} için istatistikler alındı.")
        except Exception as e:
            print(f"Game ID {game_id} için hata: {e}")

        # API sınırına takılmamak için bekleme süresi ekle
        time.sleep(0.5)

    # Tüm istatistikleri birleştir
    if all_stats:
        final_df = pd.concat(all_stats, ignore_index=True)
        final_df = final_df.dropna(axis=0, how='all')  # Boş satırları temizle
        return final_df
    else:
        return pd.DataFrame()  # Eğer veri yoksa boş DataFrame döndür


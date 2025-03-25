import pandas as pd


def analyze_player_stats(betting_df, stats_df):
    """
    Bahis pivot DataFrame'indeki (betting_df) baremleri, oyuncunun (PLAYER_NAME)
    stats_df'deki son 5 ve 10 maç istatistikleriyle karşılaştırır.

    Sonuç DataFrame sütunları:
      - PLAYER_NAME, TEAM, home_team, away_team, game_time_est, STAT_TYPE, BAREM
      - AVG_X5, ALT_X5, UST_X5
      - AVG_X10, ALT_X10, UST_X10
      - P_X5, P_X10, P_X (Prediction sütunları)
      - FI (Flame-Ice sütunu: Son kaç maçtır üst üste baremin üstünde veya altında olduğu)
    """
    # Tarihleri dönüştürelim
    betting_df['game_time_est'] = pd.to_datetime(betting_df['game_time_est'])
    stats_df['GAME_DATE'] = pd.to_datetime(stats_df['GAME_DATE']).dt.date

    # Oyuncuların takım bilgisini alalım
    latest_teams = {}
    for player in stats_df['PLAYER_NAME'].unique():
        player_games = stats_df[stats_df['PLAYER_NAME'] == player].sort_values('GAME_DATE', ascending=False)
        if not player_games.empty:
            latest_game = player_games.iloc[0]  # En güncel maçı al
            matchup = latest_game['MATCHUP']
            team = matchup.split(" @ ")[0] if " @ " in matchup else matchup.split(" vs. ")[0]
            latest_teams[player] = team
        else:
            latest_teams[player] = "Unknown"

    # Base sütunları belirleyelim
    base_cols = ['PLAYER_NAME', 'home_team', 'away_team', 'game_time_est']
    stat_types = [col for col in betting_df.columns if col not in base_cols]

    result_rows = []
    for _, row in betting_df.iterrows():
        player = row['PLAYER_NAME']
        home_team = row['home_team']
        away_team = row['away_team']
        game_time = row['game_time_est']

        for stat in stat_types:
            barem = row[stat]
            player_games = stats_df[stats_df['PLAYER_NAME'] == player].copy()
            if player_games.empty:
                continue

            player_games = player_games.sort_values('GAME_DATE', ascending=False)

            # Son 5 ve 10 maçlık veriyi çekelim
            last_5_games = player_games.head(5)
            last_10_games = player_games.head(10)

            # Fonksiyon ile hesaplama yapalım
            def compute_stats(games, barem):
                over_count, under_count, total_stat, valid_games = 0, 0, 0, 0
                outcomes = []

                for _, game in games.iterrows():
                    # Birleşik istatistikleri kontrol et
                    if "_" in stat:
                        components = stat.split("_")
                        actual = sum(game.get(comp, 0) for comp in components if comp in game)
                    else:
                        actual = game.get(stat, None)

                    if pd.isna(actual):
                        continue

                    total_stat += actual
                    valid_games += 1

                    if actual > barem:
                        over_count += 1
                        outcomes.append('over')
                    elif actual < barem:
                        under_count += 1
                        outcomes.append('under')

                avg_stat = total_stat / valid_games if valid_games > 0 else None
                return avg_stat, over_count, under_count, outcomes

            # 5 ve 10 maçlık istatistikleri hesapla
            AVG_X5, UST_X5, ALT_X5, _ = compute_stats(last_5_games, barem)
            AVG_X10, UST_X10, ALT_X10, outcomes_10 = compute_stats(last_10_games, barem)

            # FI (Flame-Ice) hesaplama
            FI = 0
            if outcomes_10:
                streak_type = outcomes_10[0]  # Son maçtaki durum (over/under)
                for outcome in outcomes_10:
                    if outcome == streak_type:
                        FI += 1 if streak_type == 'over' else -1
                    else:
                        break

            # Prediction hesaplama
            P_X5 = "UNDER" if ALT_X5 in [4, 5] else "OVER" if UST_X5 in [4, 5] else ""
            P_X10 = "UNDER" if ALT_X10 >= 8 else "OVER" if UST_X10 >= 8 else ""
            P_X = P_X5 if P_X5 == P_X10 and P_X5 != "" else ""

            # Sonuçları ekleyelim
            result_rows.append({
                'PLAYER_NAME': player,
                'TEAM': latest_teams.get(player, "Unknown"),
                'home_team': home_team,
                'away_team': away_team,
                'game_time_est': game_time,
                'STAT_TYPE': stat,
                'BAREM': barem,
                'AVG_X5': AVG_X5,
                'ALT_X5': ALT_X5,
                'UST_X5': UST_X5,
                'AVG_X10': AVG_X10,
                'ALT_X10': ALT_X10,
                'UST_X10': UST_X10,
                'FI': FI,
                'P_X5': P_X5,
                'P_X10': P_X10,
                'P_X': P_X

            })

    result_df = pd.DataFrame(result_rows)
    return result_df

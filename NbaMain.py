import os
import pandas as pd
import numpy as np
from NbaPlayerProps2 import get_nba_player_props
from NbaPlayerStats2 import fetch_nba_stats
from AnalyzeNba import analyze_player_stats
from postgres_upload import insert_unique_df_to_postgres
from dotenv import load_dotenv

load_dotenv()

# Pandas ayarları (Tüm satır ve sütunları göstermek için)
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.set_option('display.width', None)  # Ekranda genişlik sınırlamasını kaldır

# API Key
API_KEY = os.getenv("API_KEY")

# 1️⃣ Bahis oranlarını çek
df_odds = get_nba_player_props(API_KEY)

# 2️⃣ Oyuncu listesi
player_list = df_odds["PLAYER_NAME"].unique().tolist()

# 3️⃣ Oyuncuların maç istatistiklerini çek
df_stats = fetch_nba_stats(player_list)

# 4️⃣ Analiz yap
df_analysis = analyze_player_stats(df_odds, df_stats)
df_analysis.dropna(subset=["BAREM"], inplace=True)

# 5️⃣ Sonuçları ekrana yazdır
print("Oyuncu istatistik analizi:")
print(df_analysis)

# 6️⃣ Excel'e kaydet
with pd.ExcelWriter("nba_data_analysis.xlsx", engine="openpyxl") as writer:
    df_odds.to_excel(writer, sheet_name="Betting Data", index=False)
    df_stats.to_excel(writer, sheet_name="Player Stats", index=False)
    df_analysis.to_excel(writer, sheet_name="Analysis", index=False)

print("Analiz sonuçları başarıyla Excel formatında kaydedildi.")

# 7️⃣ Local PostgreSQL'e veri yükle (tekrarlayanları atla)
df_stats = df_stats.copy()
df_analysis = df_analysis.copy()

insert_unique_df_to_postgres(df_stats, "gamestats", conflict_cols=["player_id", "game_id"])
insert_unique_df_to_postgres(df_analysis, "prediction", conflict_cols=["player_name", "game_time_est", "stat_type"])

print("Analiz sonuçları local PostgreSQL veritabanına kaydedildi.")

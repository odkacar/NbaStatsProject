import pandas as pd
import numpy as np
from NbaPlayerProps2 import get_nba_player_props
from NbaPlayerStats2 import fetch_nba_stats, fetch_last_24h_nba_stats
from AnalyzeNba import analyze_player_stats
from insert_data_to_mssql import insert_data_to_mssql


# Pandas ayarları (Tüm satır ve sütunları göstermek için)
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.set_option('display.width', None)  # Ekranda genişlik sınırlamasını kaldır

# API Key'inizi buraya girin
API_KEY = "7d1064b626d71e22138968960da1706f"

# 1️⃣ Bahis oranlarını çek
df_odds = get_nba_player_props(API_KEY)

# 2️⃣ Bahis oranlarında geçen oyuncuların listesini oluştur
player_list = df_odds["PLAYER_NAME"].unique().tolist()

# 3️⃣ Sadece bahis oranlarında geçen oyuncuların istatistiklerini çek
df_stats = fetch_nba_stats(player_list)

# 4️⃣ Bahis verileri ile oyuncu istatistiklerini analiz et
df_analysis = analyze_player_stats(df_odds, df_stats)

# 5️⃣ Sonuçları ekrana yazdır
print("Oyuncu istatistik analizi:")
print(df_analysis)

# 6️⃣ Sonuçları Excel formatında kaydet (CSV yerine .xlsx)
with pd.ExcelWriter("nba_data_analysis.xlsx", engine="openpyxl") as writer:
    df_odds.to_excel(writer, sheet_name="Betting Data", index=False)
    df_stats.to_excel(writer, sheet_name="Player Stats", index=False)
    df_analysis.to_excel(writer, sheet_name="Analysis", index=False)

print("Analiz sonuçları başarıyla Excel formatında kaydedildi.")

insert_data_to_mssql(df_analysis, "Prediction")

print("Analiz sonuçları Database'e kayıt edildi")
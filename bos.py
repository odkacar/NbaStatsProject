import pyodbc
import pandas as pd
from fetch_nba_stats_last_24h import fetch_nba_stats_last_24h

def update_prediction_results():
    """
    Prediction tablosundaki tahminleri, gerçek maç sonuçları ile eşleştirerek
    PredictionSonuc tablosuna kaydeder. Aynı tahmini tekrar eklememek için kontrol yapar.
    """

    # MSSQL Bağlantı bilgileri (Kendi sunucu bilgilerini gir)
    server = "localhost"  # MSSQL Sunucu adı (örn: YOUR_SERVER\SQLEXPRESS)
    database = "YourDatabaseName"
    username = "sa"  # Windows Authentication kullanıyorsan boş bırak
    password = "YourPassword"  # Windows Authentication kullanıyorsan boş bırak
    driver = "{ODBC Driver 17 for SQL Server}"

    # Bağlantıyı oluştur
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Prediction tablosundaki tüm oyuncuları al
    cursor.execute("SELECT * FROM Prediction")
    prediction_data = cursor.fetchall()

    if not prediction_data:
        print("Prediction tablosunda tahmin bulunamadı!")
        return

    # Prediction tablosunu Pandas DataFrame'e çevir
    columns = [desc[0] for desc in cursor.description]
    df_prediction = pd.DataFrame(prediction_data, columns=columns)

    # Dünkü gerçek maç istatistiklerini çek
    df_actual = fetch_nba_stats_last_24h()

    if df_actual.empty:
        print("Son 24 saatte oynanan maç bulunamadı!")
        return

    # NBA API'den gelen istatistik kolon isimleri (Bu isimler API'ye bağlı)
    nba_stat_map = {
        "Points": "PTS",
        "Rebounds": "REB",
        "Assists": "AST",
        "Steals": "STL",
        "Blocks": "BLK",
        "Turnovers": "TOV",
        "Field Goals Made": "FGM",
        "Field Goals Attempted": "FGA",
        "Three Pointers Made": "FG3M",
        "Three Pointers Attempted": "FG3A",
        "Free Throws Made": "FTM",
        "Free Throws Attempted": "FTA",
    }

    # `df_actual["GAME_DATE"]` → Datetime formatına çevir ve sadece gün, ay, yıl al
    df_actual["GAME_DATE"] = pd.to_datetime(df_actual["GAME_DATE"]).dt.date

    # DataFrame'leri eşleştirerek `SonucStat` ve `Sonuc` hesapla
    results = []
    for _, pred_row in df_prediction.iterrows():
        player_name = pred_row["PLAYER_NAME"]
        game_day = pred_row["game_time_est"].date()
        stat_type = pred_row["STAT_TYPE"]
        barem = pred_row["BAREM"]

        # STAT_TYPE'ı NBA API'den gelen sütun adına çevir
        nba_stat_column = nba_stat_map.get(stat_type, None)

        if nba_stat_column is None:
            print(f"Uyarı: '{stat_type}' için NBA API'de eşleşen istatistik bulunamadı!")
            continue

        # Dünkü maçlardan ilgili oyuncunun verisini bul
        actual_stat = df_actual[(df_actual["PLAYER_NAME"] == player_name) & (df_actual["GAME_DATE"] == game_day)]

        if not actual_stat.empty:
            # `SonucStat` = Gerçekleşen istatistik
            actual_value = actual_stat.iloc[0][nba_stat_column]

            # `Sonuc` = Tahminin başarılı olup olmadığını belirle (Üst/Alt)
            result = "ÜST" if actual_value > barem else "ALT"

            # PredictionSonuc tablosuna eklemek için listeye kaydet
            results.append((
                player_name, pred_row["TEAM"], pred_row["home_team"], pred_row["away_team"],
                pred_row["game_time_est"], stat_type, barem, pred_row["AVG_X5"], pred_row["ALT_X5"],
                pred_row["UST_X5"], pred_row["AVG_X10"], pred_row["ALT_X10"], pred_row["UST_X10"],
                pred_row["FI"], pred_row["P_X5"], pred_row["P_X10"], pred_row["P_X"],
                actual_value, result
            ))

    # Eğer yeni sonuçlar varsa MSSQL'e ekle (Duplicate kontrolü ile)
    if results:
        insert_query = """
        IF NOT EXISTS (
            SELECT 1 FROM PredictionSonuc 
            WHERE PLAYER_NAME = ? AND game_time_est = ? AND STAT_TYPE = ? AND BAREM = ?
        )
        INSERT INTO PredictionSonuc (
            PLAYER_NAME, TEAM, home_team, away_team, game_time_est, STAT_TYPE, BAREM, AVG_X5,
            ALT_X5, UST_X5, AVG_X10, ALT_X10, UST_X10, FI, P_X5, P_X10, P_X, SonucStat, Sonuc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        for row in results:
            cursor.execute(insert_query, (row[0], row[4], row[5], row[6]) + row)

        conn.commit()
        print("PredictionSonuc tablosuna yeni veriler başarıyla eklendi!")

    else:
        print("Eşleşen veri bulunamadı!")

    # Bağlantıyı kapat
    cursor.close()
    conn.close()

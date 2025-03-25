import pyodbc


def insert_data_to_mssql(df_data, table_name):
    """
    MSSQL'e veri ekleyen fonksiyon.
    Duplicate kayıtları kontrol ederek sadece yeni verileri ekler.

    :param df_data: Pandas DataFrame, MSSQL'e eklenecek veriler
    :param table_name: str, MSSQL'deki hedef tablo adı
    """

    # MSSQL Bağlantı bilgileri (Kendi bilgilerinle güncelle)
    server = "localhost"  # MSSQL Sunucu adı (örn: YOUR_SERVER\SQLEXPRESS)
    database = "NBA"
    username = ""  # Windows Authentication kullanıyorsan boş bırak
    password = ""  # Windows Authentication kullanıyorsan boş bırak
    driver = "{ODBC Driver 17 for SQL Server}"  # Eğer farklı bir sürüm kullanıyorsan değiştir

    # Bağlantıyı oluştur
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Sütun isimlerini belirle (Tüm sütunlar için dinamik olarak çalışır)
    columns = df_data.columns.tolist()
    col_names = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])

    # Duplicate kontrolü için UNIQUE olan sütunları belirle
    unique_columns = ["PLAYER_NAME", "game_day", "STAT_TYPE", "BAREM"]

    # UNIQUE sütunları kullanarak EXISTS kontrolü için SQL
    exists_check = " AND ".join([f"{col} = ?" for col in unique_columns])

    # SQL Insert Sorgusu (Duplicate Kayıtları Önleyen)
    insert_query = f"""
    IF NOT EXISTS (
        SELECT 1 FROM {table_name} WHERE {exists_check}
    )
    INSERT INTO {table_name} ({col_names})
    VALUES ({placeholders});
    """

    # DataFrame'deki verileri döngü ile MSSQL'e ekleyelim
    for _, row in df_data.iterrows():
        cursor.execute(insert_query, tuple(row[unique_columns]) + tuple(row))

    # Değişiklikleri kaydet
    conn.commit()
    print(f"{table_name} tablosuna yeni veriler başarıyla eklendi! (Duplicate kayıtlar önlendi)")

    # Bağlantıyı kapat
    cursor.close()
    conn.close()

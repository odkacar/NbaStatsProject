import psycopg2

# Supabase PostgreSQL bağlantı bilgilerini buraya gir
DATABASE_URL = "postgresql://postgres:NbaStatsFarm01@db.cvzbaumklrdyfifozfwu.supabase.co:5432/postgres"

# PostgreSQL bağlantısını kur
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Prediction tablosunu oluştur
create_table_query = """
CREATE TABLE IF NOT EXISTS NbaPrediction (
    id SERIAL PRIMARY KEY,
    PLAYER_NAME TEXT,
    TEAM TEXT,
    home_team TEXT,
    away_team TEXT,
    game_time_est TIMESTAMP,
    STAT_TYPE TEXT,
    BAREM FLOAT,
    AVG_X5 FLOAT,
    ALT_X5 INTEGER,
    UST_X5 INTEGER,
    AVG_X10 FLOAT,
    ALT_X10 INTEGER,
    UST_X10 INTEGER,
    FI INTEGER,
    P_X5 TEXT,
    P_X10 TEXT,
    P_X TEXT
);
"""

cursor.execute(create_table_query)
conn.commit()

print("Prediction tablosu başarıyla oluşturuldu!")
cursor.close()
conn.close()

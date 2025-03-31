import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.sql import text as sql_text
from dotenv import load_dotenv

load_dotenv()

# Çevre değişkenlerinden bilgileri al
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nba")

# SQLAlchemy engine
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)

def insert_unique_df_to_postgres(df: pd.DataFrame, table_name: str, conflict_cols=None):
    """
    DataFrame'i local PostgreSQL'e insert eder. conflict_cols varsa, aynı kayıtlar atlanır (UPSERT gibi davranmaz, sadece INSERT IGNORE).
    """
    try:
        df.columns = [col.lower() for col in df.columns]
        df = df.replace({"": None})
        df = df.replace({pd.NA: None, pd.NaT: None})
        df = df.drop_duplicates(subset=conflict_cols) if conflict_cols else df

        with engine.begin() as connection:
            for _, row in df.iterrows():
                values = row.to_dict()
                keys = values.keys()
                columns = ', '.join(keys)
                placeholders = ', '.join([f':{k}' for k in keys])
                conflict = f"ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING" if conflict_cols else ""
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) {conflict};"
                connection.execute(sql_text(sql), values)

        print(f"'{table_name}' tablosuna {len(df)} satır insert edildi (var olanlar atlandı).")
    except Exception as e:
        print(f"'{table_name}' tablosuna veri insert edilirken hata: {e}")

def test_table_preview(table_name: str, limit: int = 5):
    """
    Belirtilen tabloyu test eder: kaç satır olduğunu gösterir ve örnek veri döker.
    """
    try:
        with engine.connect() as connection:
            count_result = connection.execute(sql_text(f"SELECT COUNT(*) FROM {table_name};"))
            count = count_result.scalar()
            print(f"'{table_name}' tablosunda toplam {count} kayıt var.")

            preview_result = connection.execute(sql_text(f"SELECT * FROM {table_name} LIMIT {limit};"))
            rows = preview_result.fetchall()
            if rows:
                df_preview = pd.DataFrame(rows, columns=preview_result.keys())
                print(f"'{table_name}' tablosundan ilk {limit} kayıt:")
                print(df_preview)
            else:
                print(f"'{table_name}' tablosunda veri yok.")
    except Exception as e:
        print(f"'{table_name}' tablosu okunurken hata oluştu: {e}")

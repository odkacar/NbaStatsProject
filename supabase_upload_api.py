from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from datetime import date, datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def _sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.lower() for col in df.columns]
    df = df.replace({"": None})              # Boş stringleri None yap
    df = df.replace({np.nan: None})          # NaN'leri None yap
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].apply(lambda x: isinstance(x, (pd.Timestamp, np.datetime64, date, datetime))).any():
            df[col] = df[col].astype(str)
    return df

def insert_df_to_supabase_api(df: pd.DataFrame, table_name: str):
    try:
        df = _sanitize_dataframe(df)
        data = df.to_dict(orient="records")
        response = supabase.table(table_name).insert(data).execute()
        print(f"'{table_name}' tablosuna {len(data)} satır INSERT edildi.")
    except Exception as e:
        print(f"'{table_name}' tablosuna INSERT hatası: {e}")


def upsert_df_to_supabase_api(df: pd.DataFrame, table_name: str, conflict_cols=None):
    try:
        if not table_name:
            raise ValueError("Table name is None. Please provide a valid table name.")

        df = _sanitize_dataframe(df)

        # If conflict columns are provided
        if conflict_cols:
            # Ensure the conflict_cols is a list
            if isinstance(conflict_cols, str):
                conflict_cols = [conflict_cols]

            # Drop duplicates based on conflict columns to avoid internal conflicts
            df = df.drop_duplicates(subset=conflict_cols, keep='first')

            # Get existing data for these conflict columns
            # This helps us identify which rows might cause conflicts
            query = ", ".join([f"{col}" for col in conflict_cols])
            existing_data = supabase.table(table_name).select(query).execute()

            if hasattr(existing_data, 'data') and existing_data.data:
                # Convert to DataFrame for easier comparison
                existing_df = pd.DataFrame(existing_data.data)

                # Split the dataframe into rows for insert and rows for update
                if not existing_df.empty:
                    # Create a merge key for comparison
                    df['_merge_key'] = df[conflict_cols].astype(str).agg('-'.join, axis=1)
                    existing_df['_merge_key'] = existing_df[conflict_cols].astype(str).agg('-'.join, axis=1)

                    # Identify rows to update (already exist) and rows to insert (new)
                    update_mask = df['_merge_key'].isin(existing_df['_merge_key'])
                    rows_to_update = df[update_mask].drop('_merge_key', axis=1)
                    rows_to_insert = df[~update_mask].drop('_merge_key', axis=1)

                    # Handle updates
                    if not rows_to_update.empty:
                        update_data = rows_to_update.to_dict(orient="records")
                        # Use smaller batches for updates
                        batch_size = 50
                        for i in range(0, len(update_data), batch_size):
                            batch = update_data[i:i + batch_size]
                            supabase.table(table_name).upsert(batch, on_conflict=conflict_cols).execute()
                        print(f"Updated {len(rows_to_update)} existing rows in '{table_name}'")

                    # Handle inserts
                    if not rows_to_insert.empty:
                        insert_data = rows_to_insert.to_dict(orient="records")
                        supabase.table(table_name).insert(insert_data).execute()
                        print(f"Inserted {len(rows_to_insert)} new rows in '{table_name}'")

                    print(f"'{table_name}' tablosuna toplam {len(df)} satır işlendi.")
                    return

            # If we can't get existing data or there's none, fall back to regular batched upsert
            data = df.to_dict(orient="records")
            batch_size = 25  # Smaller batch size
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                supabase.table(table_name).upsert(batch, on_conflict=conflict_cols).execute()

            print(f"'{table_name}' tablosuna {len(data)} satır UPSERT edildi.")
        else:
            # If no conflict columns specified, just do a regular insert
            data = df.to_dict(orient="records")
            supabase.table(table_name).insert(data).execute()
            print(f"'{table_name}' tablosuna {len(data)} satır INSERT edildi.")

    except Exception as e:
        print(f"'{table_name}' tablosuna UPSERT hatası: {e}")
        raise  # Re-raise the exception to see the full traceback
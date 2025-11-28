import pandas as pd
import uuid 
import datetime

def normalize_column_name_and_type(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def add_ingest_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    meta = {
        "ingest_time": datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
        "ingest_id": str(uuid.uuid4()),
        "source": source,
    }
    # metadata as separate file or columns — here I added columns for traceability
    df["_ingest_time"] = meta["ingest_time"]
    df["_ingest_id"] = meta["ingest_id"]
    df["_source"] = meta["source"]
    return df
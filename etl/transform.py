# etl/transform.py
import pandas as pd

def transform_matches(df):
    """
    Minimal transforms on already-clean data:
      - ensure types
      - rename if necessary (should already be renamed by extract)
      - compute a tiny metric: goal_difference
    """
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # convert scores to integer (they may be None)
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").fillna(0).astype(int)
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").fillna(0).astype(int)

    df["goal_difference"] = (df["home_score"] - df["away_score"]).abs()

    return df

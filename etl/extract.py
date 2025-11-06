# etl/extract.py
import requests
import pandas as pd
from config.config import API_URL, API_KEY

def extract_matches(limit=None):
    """
    Fetch matches from the API and normalize into a DataFrame.
    limit: optional int to limit number of rows returned (useful for dev/test)
    """
    headers = {"X-Auth-Token": API_KEY} if API_KEY and API_KEY != "YOUR_API_KEY_HERE" else {}
    resp = requests.get(API_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()

    matches = payload.get("matches", [])
    if not matches:
        return pd.DataFrame()  # empty DF if nothing returned

    df = pd.json_normalize(matches)

    # Keep only already-clean fields we need
    keep_cols = [
        "id",
        "utcDate",
        "homeTeam.name",
        "awayTeam.name",
        "score.fullTime.homeTeam",
        "score.fullTime.awayTeam"
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()

    # rename columns to friendly names
    df.columns = ["match_id", "date", "home_team", "away_team", "home_score", "away_score"]

    if limit:
        df = df.head(limit)

    return df

if __name__ == "__main__":
    # quick test run
    df = extract_matches(limit=10)
    print(df.head())
    print(f"rows: {len(df)}")

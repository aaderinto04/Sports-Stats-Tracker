# etl/load.py
import os
import snowflake.connector
from config.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
)

def create_matches_table_if_not_exists(conn):
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.matches (
        match_id STRING PRIMARY KEY,
        date DATE,
        home_team STRING,
        away_team STRING,
        home_score INT,
        away_score INT,
        goal_difference INT
    );
    """)
    cur.close()

def load_to_snowflake(df, dry_run=True):
    """
    Load DataFrame to Snowflake. By default dry_run=True will just print first rows.
    Set dry_run=False once Snowflake config is set and you want to actually push data.
    """
    if df.empty:
        print("No rows to load.")
        return

    if dry_run:
        print("DRY RUN MODE: sample rows to be loaded:")
        print(df.head().to_string(index=False))
        return

    # Actual load
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    create_matches_table_if_not_exists(conn)

    cur = conn.cursor()
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_SCHEMA}.matches (match_id, date, home_team, away_team, home_score, away_score, goal_difference)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, (
            str(row["match_id"]),
            row["date"].date() if not pd.isna(row["date"]) else None,
            row["home_team"],
            row["away_team"],
            int(row["home_score"]),
            int(row["away_score"]),
            int(row["goal_difference"])
        ))

    conn.commit()
    cur.close()
    conn.close()

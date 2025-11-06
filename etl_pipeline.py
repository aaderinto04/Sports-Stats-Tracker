# etl_pipeline.py
from etl.extract import extract_matches
from etl.transform import transform_matches
from etl.load import load_to_snowflake
from visuals.charts import plot_goal_difference

def main():
    print("1) Extracting")
    raw = extract_matches(limit=50)   # limit small for development

    print("2) Transforming")
    clean = transform_matches(raw)

    print("3) Load (dry run)")
    load_to_snowflake(clean, dry_run=True)

    print("4) Visualization")
    plot_goal_difference(clean)

if __name__ == "__main__":
    main()

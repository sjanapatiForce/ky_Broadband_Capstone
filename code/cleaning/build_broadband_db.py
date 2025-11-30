import sqlite3
import pandas as pd

# -----------------------------
# FILE PATHS (update if needed)
# -----------------------------
PATH_PROVIDER = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\Provider_summary_by_county.csv"
PATH_COUNTY   = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\ky_bdc_demographics_final_dataset.csv"
PATH_H3       = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\bdc_h3_points.csv"

DB_PATH       = r"H:\Broadband_Project_1\analysis\broadband_ky.db"


def main():
    # -----------------------------
    # LOAD DATAFRAMES
    # -----------------------------
    provider_df = pd.read_csv(
        PATH_PROVIDER,
        dtype={"county_fips": str, "provider_id": str}
    )
    county_df = pd.read_csv(
        PATH_COUNTY,
        dtype={"county_fips": str}
    )
    h3_df = pd.read_csv(
        PATH_H3,
        dtype={"county_fips": str, "h3_res8_id": str}
    )

    # Ensure 5-digit county_fips
    provider_df["county_fips"] = provider_df["county_fips"].astype(str).str.zfill(5)
    county_df["county_fips"]   = county_df["county_fips"].astype(str).str.zfill(5)
    h3_df["county_fips"]       = h3_df["county_fips"].astype(str).str.zfill(5)

    print("Provider rows (raw):", len(provider_df))
    print("County rows:", len(county_df))
    print("H3 rows (raw):", len(h3_df))

    # -----------------------------
    # CONNECT TO SQLITE
    # -----------------------------
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Enforce foreign keys
    cur.execute("PRAGMA foreign_keys = ON;")

    # -----------------------------
    # DROP TABLES IF THEY EXIST
    # -----------------------------
    cur.execute("DROP TABLE IF EXISTS hex_coverage;")
    cur.execute("DROP TABLE IF EXISTS provider_summary_by_county;")
    cur.execute("DROP TABLE IF EXISTS county_summary;")

    # -----------------------------
    # CREATE TABLES
    # -----------------------------

    # 1) COUNTY_SUMMARY
    cur.execute(
        """
        CREATE TABLE county_summary (
            county_fips TEXT PRIMARY KEY,
            county_name TEXT,

            county_avg_down          REAL,
            county_min_provider_down REAL,
            county_max_provider_down REAL,
            total_locations          INTEGER,
            underserved_locations    INTEGER,
            pct_underserved          REAL,
            provider_count           INTEGER,
            providers_below100       INTEGER,

            Less_Than_9th_grade      INTEGER,
            Less_Than_HighSchool     INTEGER,
            Atleast_Bachelors        INTEGER,
            Median_Household_Income  REAL,
            Population               INTEGER,
            total_est_poverty        INTEGER,

            area_sq_mi               REAL,
            desktop_laptop_estimate  INTEGER,
            smartphone_estimate      INTEGER
        );
        """
    )

    # 2) PROVIDER_SUMMARY_BY_COUNTY
    cur.execute(
        """
        CREATE TABLE provider_summary_by_county (
            provider_county_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            county_fips           TEXT NOT NULL,
            county_name           TEXT,
            provider_id           TEXT NOT NULL,
            provider_name         TEXT,

            avg_down              REAL,
            avg_up                REAL,
            locations             INTEGER,
            underserved_locations INTEGER,
            locations_below100    INTEGER,

            FOREIGN KEY (county_fips) REFERENCES county_summary(county_fips),
            UNIQUE (county_fips, provider_id)
        );
        """
    )

    # 3) HEX_COVERAGE
    cur.execute(
        """
        CREATE TABLE hex_coverage (
            hex_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            h3_res8_id      TEXT NOT NULL,
            county_fips     TEXT NOT NULL,

            lat             REAL,
            lon             REAL,
            max_down        REAL,
            max_up          REAL,
            provider_count  INTEGER,
            provider_names  TEXT,
            tech_types      TEXT,
            service_category TEXT,

            FOREIGN KEY (county_fips) REFERENCES county_summary(county_fips),
            UNIQUE (h3_res8_id)
        );
        """
    )

    conn.commit()

    # -----------------------------
    # INSERT INTO county_summary
    # -----------------------------
    county_cols = [
        "county_fips",
        "county_name",
        "county_avg_down",
        "county_min_provider_down",
        "county_max_provider_down",
        "total_locations",
        "underserved_locations",
        "pct_underserved",
        "provider_count",
        "providers_below100",
        "Less_Than_9th_grade",
        "Less_Than_HighSchool",
        "Atleast_Bachelors",
        "Median_Household_Income",
        "Population",
        "total_est_poverty",
        "area_sq_mi",
        "desktop_laptop_estimate",
        "smartphone_estimate",
    ]

    # Clean numeric-like columns that might have commas
    numeric_like_cols = [
        "Median_Household_Income",
        "Population",
        "total_est_poverty",
        "desktop_laptop_estimate",
        "smartphone_estimate",
        "total_locations",
        "underserved_locations",
        "providers_below100",
        "Less_Than_9th_grade",
        "Less_Than_HighSchool",
        "Atleast_Bachelors",
    ]
    for col in numeric_like_cols:
        if col in county_df.columns:
            county_df[col] = (
                county_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace("nan", None)
            )

    county_df[county_cols].to_sql(
        "county_summary", conn, if_exists="append", index=False
    )

    # -----------------------------
    # INSERT INTO provider_summary_by_county
    # -----------------------------
    provider_cols = [
        "county_fips",
        "county_name",
        "provider_id",
        "provider_name",
        "avg_down",
        "avg_up",
        "locations",
        "underserved_locations",
        "locations_below100",
    ]

    for col in ["locations", "underserved_locations", "locations_below100"]:
        if col in provider_df.columns:
            provider_df[col] = (
                provider_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace("nan", None)
            )

    # Deduplicate (county_fips, provider_id) to satisfy UNIQUE constraint
    provider_df = provider_df.sort_values(["county_fips", "provider_id"])
    provider_df = provider_df.drop_duplicates(
        subset=["county_fips", "provider_id"], keep="first"
    )

    print("Provider rows after dedup:", len(provider_df))

    provider_df[provider_cols].to_sql(
        "provider_summary_by_county", conn, if_exists="append", index=False
    )

    # -----------------------------
    # INSERT INTO hex_coverage
    # -----------------------------
    h3_cols = [
        "h3_res8_id",
        "county_fips",
        "lat",
        "lon",
        "max_down",
        "max_up",
        "provider_count",
        "provider_names",
        "tech_types",
        "service_category",
    ]

    # Deduplicate by h3_res8_id to satisfy UNIQUE constraint
    h3_df = h3_df.sort_values(["h3_res8_id", "county_fips"])
    h3_df = h3_df.drop_duplicates(subset=["h3_res8_id"], keep="first")

    print("H3 rows after dedup:", len(h3_df))

    h3_df[h3_cols].to_sql(
        "hex_coverage", conn, if_exists="append", index=False
    )

    conn.commit()

    # -----------------------------
    # SANITY CHECK COUNTS
    # -----------------------------
    print("\nRow counts in SQLite:")
    for table in ["county_summary", "provider_summary_by_county", "hex_coverage"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        print(f"  {table}: {cnt}")

    conn.close()
    print("\nSQLite database created at:", DB_PATH)


if __name__ == "__main__":
    main()

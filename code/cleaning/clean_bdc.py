import pandas as pd

in_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_all_raw.csv"
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_clean_step2.csv"

df = pd.read_csv(in_path, dtype=str)

print("Initial rows:", len(df))

# ------------------------------------------------
# 1. KEEP ONLY NECESSARY COLUMNS
# ------------------------------------------------
keep_cols = [
    "provider_id",
    "brand_name",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
    "block_geoid"
]
df = df[keep_cols]

# ------------------------------------------------
# 2. REMOVE MISSING GEOID OR SPEED INFO
# ------------------------------------------------
df = df.dropna(subset=[
    "block_geoid",
    "max_advertised_download_speed",
    "max_advertised_upload_speed"
])

# ------------------------------------------------
# 3. CONVERT SPEEDS TO NUMERIC
# ------------------------------------------------
df["maxDown"] = pd.to_numeric(df["max_advertised_download_speed"], errors="coerce")
df["maxUp"]   = pd.to_numeric(df["max_advertised_upload_speed"], errors="coerce")

# drop rows where conversion failed
df = df.dropna(subset=["maxDown", "maxUp"])

# ------------------------------------------------
# 4. ADD COUNTY FIPS (first 5 digits of block_geoid)
# ------------------------------------------------
df["county_fips"] = df["block_geoid"].str[:5]

# ------------------------------------------------
# 5. KEEP ONLY KENTUCKY (FIPS starts with '21')
# ------------------------------------------------
df = df[df["county_fips"].str.startswith("21")]

print("Rows after KY filter:", len(df))

# ------------------------------------------------
# 6. SAVE CLEANED FILE
# ------------------------------------------------
df.to_csv(out_path, index=False)
print("Cleaned file saved to:", out_path)

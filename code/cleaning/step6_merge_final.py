import pandas as pd
import numpy as np

# ---------------------------
# Helper: load file and rename FIPS column to county_fips
# ---------------------------
def load_with_fips(path):
    df = pd.read_csv(path, dtype=str)
    
    # Drop junk unnamed columns if any
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    
    # detect FIPS column from any of these possibilities:
    for col in df.columns:
        if col.lower() in ["fips", "county_fips", "geoid"]:
            df = df.rename(columns={col: "county_fips"})
            break

    if "county_fips" not in df.columns:
        raise ValueError(f"No FIPS-like column found in {path}")
    
    # Ensure 5-digit county_fips
    df["county_fips"] = df["county_fips"].astype(str).str[:5]
    return df

# ---------------------------
# INPUT PATHS
# ---------------------------
bdc_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step5_county_agg.csv"

edu_path  = r"H:\Broadband_Project_1\datasets\Cleaned_Census_data\Cleaned_EDU_Attainments_CountyWise.csv"
inc_path  = r"H:\Broadband_Project_1\datasets\Cleaned_Census_data\Median_Household_Income_KY_Countywise.csv"
pop_path  = r"H:\Broadband_Project_1\datasets\Cleaned_Census_data\Population&Poverty_KY_Countywise.csv"

area_path   = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\ky_county_area_wikipedia.csv"
device_path = r"H:\Broadband_Project_1\datasets\Cleaned_Census_data\ky_computer_smartphone_estimates_with_fips.csv"

# OUTPUT
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\ky_bdc_demographics_final_dataset.csv"

# ---------------------------
# LOAD ALL FILES
# ---------------------------
bdc = pd.read_csv(bdc_path, dtype={"county_fips": str})
bdc = bdc.loc[:, ~bdc.columns.str.startswith("Unnamed")]
bdc["county_fips"] = bdc["county_fips"].astype(str).str[:5]

edu   = load_with_fips(edu_path)
inc   = load_with_fips(inc_path)
pop   = load_with_fips(pop_path)
area  = load_with_fips(area_path)
dev   = load_with_fips(device_path)

print("Loaded:")
print("  BDC rows:", len(bdc))
print("  Education rows:", len(edu))
print("  Income rows:", len(inc))
print("  Population rows:", len(pop))
print("  Area rows:", len(area), " | columns:", area.columns.tolist())
print("  Device rows:", len(dev), " | columns:", dev.columns.tolist())

# ---------------------------
# MERGE ON county_fips
# ---------------------------
df = (
    bdc.merge(edu,  on="county_fips", how="left")
       .merge(inc,  on="county_fips", how="left")
       .merge(pop,  on="county_fips", how="left")
       .merge(area, on="county_fips", how="left")
       .merge(dev,  on="county_fips", how="left")
)

print("\nRows after merge:", len(df))
print("Columns after merge (before name cleanup):")
print(df.columns.tolist())

# ---------------------------
# BUILD SINGLE county_name COLUMN
# ---------------------------
candidate_cols = [
    "county_name", "county_name_x", "county_name_y",
    "County Name", "County", "county"
]

name_series = None
for col in candidate_cols:
    if col in df.columns:
        if name_series is None:
            name_series = df[col].copy()
        else:
            name_series = name_series.fillna(df[col])

if name_series is None:
    # fallback if nothing was found
    name_series = pd.Series([None] * len(df), index=df.index)

df["county_name"] = name_series

# ---------------------------
# DROP DUPLICATE NAME COLUMNS
# ---------------------------
cols_to_drop = []
for col in df.columns:
    lower = col.lower()
    if lower in ["county name", "county", "county_name_x", "county_name_y"]:
        cols_to_drop.append(col)

cols_to_drop = [c for c in cols_to_drop if c != "county_name"]  # keep unified
df = df.drop(columns=cols_to_drop, errors="ignore")

# ---------------------------
# REORDER COLUMNS: county_fips, county_name first
# ---------------------------
cols = list(df.columns)
if "county_fips" in cols and "county_name" in cols:
    cols.remove("county_fips")
    cols.remove("county_name")
    cols = ["county_fips", "county_name"] + cols
    df = df[cols]

print("\nColumns after name cleanup:")
print(df.columns.tolist())

# ------------------------------------------------------
# DATA CLEANING: EMPTY STRINGS, COMMAS, NUMERIC TYPES
# ------------------------------------------------------

# Replace empty / space-only strings with NaN
df = df.replace(r'^\s*$', np.nan, regex=True)
df = df.replace("", np.nan)
df = df.replace(" ", np.nan)

# Remove stray quote characters
df = df.replace('"', "", regex=True)

# Identify numeric-like columns:
#   - exclude keys / obvious text columns
exclude_for_numeric = ["county_fips", "county_name"]
numeric_cols = [
    col for col in df.columns
    if col not in exclude_for_numeric
       and not any(key in col.lower() for key in ["name", "provider"])
]

# Remove thousands separators (commas) in numeric-like columns
for col in numeric_cols:
    df[col] = df[col].astype(str).str.replace(",", "")

# Convert to numeric (coerce errors to NaN)
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

print("\nSample dtypes after numeric cleaning:")
print(df.dtypes.head(20))

# ---------------------------
# SAVE FINAL DATASET
# ---------------------------
df.to_csv(out_path, index=False)
print("\nFinal dataset saved to:")
print(out_path)

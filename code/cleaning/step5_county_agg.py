import pandas as pd

# INPUT FILES
prov_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step4_provider_agg.csv"
raw_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step3_with_flags.csv"

# OUTPUT FILE
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step5_county_agg.csv"

# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------
prov = pd.read_csv(prov_path, dtype={"county_fips": str})
raw  = pd.read_csv(raw_path,  dtype={"county_fips": str})

prov["county_fips"] = prov["county_fips"].str[:5]
raw["county_fips"]  = raw["county_fips"].str[:5]

# ----------------------------------------------------------
# 1) COUNTY AVERAGE DOWNLOAD SPEED (ALL PROVIDERS)
#    -> average of provider_avg_down across providers in the county
# ----------------------------------------------------------
county_avg = (
    prov.groupby("county_fips")["provider_avg_down"]
        .mean()
        .reset_index()
)
county_avg.columns = ["county_fips", "county_avg_down"]

# (optional but useful) min & max provider avg download per county
county_min_max = (
    prov.groupby("county_fips")["provider_avg_down"]
        .agg(["min", "max"])
        .reset_index()
)
county_min_max.columns = [
    "county_fips",
    "county_min_provider_down",
    "county_max_provider_down"
]

# ----------------------------------------------------------
# 2) COUNTY UNDERSERVED METRICS (100/20 RULE, FROM RAW)
# ----------------------------------------------------------
county_underserved = raw.groupby("county_fips").agg(
    total_locations=("block_geoid", "count"),
    underserved_locations=("is_underserved", "sum")
).reset_index()

county_underserved["pct_underserved"] = (
    county_underserved["underserved_locations"]
    / county_underserved["total_locations"]
    * 100
)

# ----------------------------------------------------------
# 3) PROVIDER COUNTS PER COUNTY
#    - provider_count: how many unique providers
#    - providers_below100: how many providers have at least one <100 Mbps location
# ----------------------------------------------------------
county_providers = prov.groupby("county_fips").agg(
    provider_count=("provider_id", "nunique"),
    providers_below100=("provider_below100_count", lambda x: (x > 0).sum())
).reset_index()

# ----------------------------------------------------------
# 4) MERGE ALL COUNTY-LEVEL METRICS
# ----------------------------------------------------------
county_final = (
    county_avg
    .merge(county_min_max,      on="county_fips", how="left")
    .merge(county_underserved,  on="county_fips", how="left")
    .merge(county_providers,    on="county_fips", how="left")
)

# ----------------------------------------------------------
# 5) SAVE RESULT
# ----------------------------------------------------------
county_final.to_csv(out_path, index=False)

print("Step 5 completed. County-level metrics saved to:")
print(out_path)

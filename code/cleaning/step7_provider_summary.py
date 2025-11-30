import pandas as pd

# INPUT FILES
prov_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step4_provider_agg.csv"
pop_path  = r"H:\Broadband_Project_1\datasets\Cleaned_Census_data\Population&Poverty_KY_Countywise.csv"

# OUTPUT FILE
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\provider_summary_by_county.csv"

# ---------------------------
# LOAD PROVIDER-LEVEL DATA
# ---------------------------
prov = pd.read_csv(prov_path, dtype={"county_fips": str})
prov["county_fips"] = prov["county_fips"].str[:5]

# ---------------------------
# LOAD COUNTY NAMES FROM POPULATION FILE
# (columns: fips, county, Population, total_est_poverty)
# ---------------------------
pop = pd.read_csv(pop_path, dtype=str)

# rename FIPS -> county_fips, county -> county_name
for col in pop.columns:
    if col.lower() in ["fips", "county_fips", "geoid"]:
        pop = pop.rename(columns={col: "county_fips"})
        break

if "county" in pop.columns:
    pop = pop.rename(columns={"county": "county_name"})
elif "County" in pop.columns:
    pop = pop.rename(columns={"County": "county_name"})

pop["county_fips"] = pop["county_fips"].str[:5]

# keep only fips + name
pop = pop[["county_fips", "county_name"]].drop_duplicates()

# ---------------------------
# MERGE TO GET COUNTY NAME
# ---------------------------
provider_summary = prov.merge(pop, on="county_fips", how="left")

# ---------------------------
# REORDER & RENAME COLUMNS
# ---------------------------
provider_summary = provider_summary[[
    "county_fips",
    "county_name",
    "provider_id",
    "brand_name",
    "provider_avg_down",
    "provider_avg_up",
    "provider_location_count",
    "provider_underserved_count",
    "provider_below100_count"
]]

provider_summary = provider_summary.rename(columns={
    "brand_name": "provider_name",
    "provider_avg_down": "avg_down",
    "provider_avg_up": "avg_up",
    "provider_location_count": "locations",
    "provider_underserved_count": "underserved_locations",
    "provider_below100_count": "locations_below100"
})

# ---------------------------
# SAVE
# ---------------------------
provider_summary.to_csv(out_path, index=False)

print("Provider summary dataset saved to:")
print(out_path)
print("Rows:", len(provider_summary))
print("Columns:", provider_summary.columns.tolist())

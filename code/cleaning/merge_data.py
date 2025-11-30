import pandas as pd
import os

# --- Paths ---
df_av_path = "combined_bdc_sample.csv"           # your BDC sample
df_blocks_path = "datasets/census_data/census_combined.csv"  # census blocks
output_dir = "datasets/merged_output"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "merged_KY_counties.csv")

# --- Load data ---
df_av = pd.read_csv(df_av_path)
df_blocks = pd.read_csv(df_blocks_path)

# -------------------------------------------------
# STEP 1: Keep only KY rows in BDC
# -------------------------------------------------
df_av = df_av[df_av['state_usps'] == 'KY']

# -------------------------------------------------
# STEP 2: Convert block_geoid to string **without losing precision**
# -------------------------------------------------
df_av['block_geoid_str'] = df_av['block_geoid'].apply(lambda x: '{:.0f}'.format(float(x)))

# County FIPS is characters 3-5 (SSCCCCTTTTTTB)
df_av['county_fips'] = df_av['block_geoid_str'].str[2:5].str.zfill(3)

# -------------------------------------------------
# STEP 3: Keep only KY census blocks
# -------------------------------------------------
df_blocks['state_code'] = df_blocks['state_code'].astype(str).str.zfill(2)
df_blocks['state_usps'] = df_blocks['state_code'].map({'21': 'KY'})
df_blocks = df_blocks[df_blocks['state_usps'] == 'KY']

# Standardize census county FIPS
df_blocks['county'] = df_blocks['county'].astype(str).str.zfill(3)

# -------------------------------------------------
# STEP 4: Extract human-readable county name
# -------------------------------------------------
df_blocks['county_name'] = (
    df_blocks['NAME']
    .str.extract(r',\s*([A-Za-z\s]+?)\s*,')[0]  # first capture group
    .str.replace(" County", "", regex=False)
)

# -------------------------------------------------
# STEP 5: Aggregate census data by county
# -------------------------------------------------
df_blocks_county = df_blocks.groupby(
    ['state_usps', 'county'], 
    as_index=False
).agg({
    'B01001_001E': 'sum',       # population
    'B25001_001E': 'sum',       # housing units
    'county_name': 'first'      # readable name
})

df_blocks_county = df_blocks_county.rename(columns={
    'B01001_001E': 'total_population',
    'B25001_001E': 'total_housing_units'
})

# -------------------------------------------------
# STEP 6: Filter BDC to only counties that exist in census
# -------------------------------------------------
valid_counties = df_blocks_county['county'].unique()
df_av = df_av[df_av['county_fips'].isin(valid_counties)]

# -------------------------------------------------
# STEP 7: Merge BDC with census
# -------------------------------------------------
merged = pd.merge(
    df_av,
    df_blocks_county,
    left_on=['state_usps', 'county_fips'],
    right_on=['state_usps', 'county'],
    how='left'
)

# -------------------------------------------------
# STEP 8: Save output
# -------------------------------------------------
merged.to_csv(output_path, index=False)
print(f"✅ KY merge completed. File saved to: {output_path}")

# Preview
print(
    merged[['state_usps', 'county_fips', 'county_name', 
            'provider_id', 'brand_name', 
            'total_population', 'total_housing_units']].head(20)
)

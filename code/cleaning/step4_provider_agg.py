import pandas as pd

in_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step3_with_flags.csv"
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step4_provider_agg.csv"

df = pd.read_csv(in_path, dtype={"county_fips": str})

# --------------------------------------------------------------------
# GROUP BY county + provider (because providers repeat across locations)
# --------------------------------------------------------------------
provider_agg = df.groupby(["county_fips", "provider_id", "brand_name"]).agg(
    provider_avg_down=("maxDown", "mean"),
    provider_avg_up=("maxUp", "mean"),
    provider_location_count=("block_geoid", "count"),
    provider_underserved_count=("is_underserved", "sum"),
    provider_below100_count=("is_below100", "sum")
).reset_index()

# --------------------------------------------------------------------
# SAVE OUTPUT
# --------------------------------------------------------------------
provider_agg.to_csv(out_path, index=False)

print("Step 4 completed. Provider-level aggregation saved to:")
print(out_path)

import pandas as pd

in_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_clean_step2.csv"
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_step3_with_flags.csv"

df = pd.read_csv(in_path, dtype={"county_fips": str})

# --------------------------------------------------------
# 1. UNDERSERVED = TRUE (1) if <100 Mbps download OR <20 Mbps upload
# --------------------------------------------------------
df["is_underserved"] = ((df["maxDown"] < 100) | (df["maxUp"] < 20)).astype(int)

# --------------------------------------------------------
# 2. BELOW100 FLAG (Download < 100 Mbps Only)
# --------------------------------------------------------
df["is_below100"] = (df["maxDown"] < 100).astype(int)

# --------------------------------------------------------
# 3. SAVE THE FILE
# --------------------------------------------------------
df.to_csv(out_path, index=False)

print("Step 3 completed. Flags added.")
print("Output:", out_path)

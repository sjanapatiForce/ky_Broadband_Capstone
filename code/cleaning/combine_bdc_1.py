import pandas as pd
import glob
import os

raw_folder = r"H:\Broadband_Project_1\datasets\bdc_data_1\raw"
out_path   = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_all_raw.csv"

# get all csvs in raw folder
files = glob.glob(os.path.join(raw_folder, "*.csv"))
print("Found files:", files)

dfs = []
for f in files:
    df_tmp = pd.read_csv(f, dtype=str)
    df_tmp["source_file"] = os.path.basename(f)  # just to know origin
    dfs.append(df_tmp)

df_all = pd.concat(dfs, ignore_index=True)
df_all.to_csv(out_path, index=False)

print("Combined file saved to:", out_path)
print("Rows:", len(df_all))

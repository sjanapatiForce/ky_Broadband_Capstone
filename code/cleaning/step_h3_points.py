import pandas as pd

# ---- handle both old and new h3-py APIs ----
try:
    # older style: from h3 import h3
    from h3 import h3 as h3lib
except ImportError:
    import h3 as h3lib

# ------------ INPUT & OUTPUT PATHS ------------
in_path  = r"H:\Broadband_Project_1\datasets\bdc_data_1\processed\bdc_all_raw.csv"
out_path = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\bdc_h3_points.csv"

# ------------ LOAD RAW BDC DATA ------------
df = pd.read_csv(in_path, dtype=str)
print("Loaded rows:", len(df))
print("Columns:", df.columns.tolist())

# Keep only Kentucky
if "state_usps" in df.columns:
    df = df[df["state_usps"] == "KY"]
print("Rows after KY filter:", len(df))

# Drop rows missing key fields
df = df.dropna(subset=[
    "h3_res8_id",
    "block_geoid",
    "max_advertised_download_speed",
    "max_advertised_upload_speed"
])
print("Rows after dropping missing key fields:", len(df))

# Convert speeds to numeric
df["maxDown"] = pd.to_numeric(df["max_advertised_download_speed"], errors="coerce")
df["maxUp"]   = pd.to_numeric(df["max_advertised_upload_speed"], errors="coerce")

df = df.dropna(subset=["maxDown", "maxUp"])
print("Rows after dropping invalid speeds:", len(df))

# Add county_fips
df["county_fips"] = df["block_geoid"].str[:5]

# ------------ TECHNOLOGY MAPPING ------------
# Technology codes (from your note):
# 40 - cable
# 10 - copper
# 50 - fiber
# 71 - Licensed fixed wireless
# 70 - unlicensed fixed wireless

TECH_MAP = {
    "40": "Cable",
    "10": "Copper",
    "50": "Fiber",
    "71": "Licensed fixed wireless",
    "70": "Unlicensed fixed wireless",
}

# df["technology"] is string because we loaded dtype=str
if "technology" in df.columns:
    df["tech_group"] = df["technology"].map(TECH_MAP).fillna("Other / Unknown")
else:
    df["tech_group"] = "Other / Unknown"

# ------------ AGGREGATE TO H3-HEX LEVEL ------------
def agg_providers(s: pd.Series) -> str:
    """Combine unique provider names into one string."""
    names = s.dropna().unique().tolist()
    return "; ".join(sorted(names))

def agg_tech(s: pd.Series) -> str:
    """Combine unique technology groups into one string."""
    vals = s.dropna().unique().tolist()
    return "; ".join(sorted(vals))

h3_grouped = df.groupby(["county_fips", "h3_res8_id"]).agg(
    max_down=("maxDown", "max"),
    max_up=("maxUp", "max"),
    provider_count=("provider_id", "nunique"),
    provider_names=("brand_name", agg_providers),
    tech_types=("tech_group", agg_tech),
).reset_index()

print("Grouped rows (unique hex cells):", len(h3_grouped))

# ------------ CLASSIFY SERVICE CATEGORY ------------
# Unserved:    <25/3
# Underserved: <100/20 (but not unserved)
# Served:      >=100 and >=20

def classify_row(row):
    down = row["max_down"]
    up   = row["max_up"]
    if pd.isna(down) or pd.isna(up):
        return "Unknown"
    if down < 25 or up < 3:
        return "Unserved"
    if down < 100 or up < 20:
        return "Underserved"
    return "Served"

h3_grouped["service_category"] = h3_grouped.apply(classify_row, axis=1)

print("Service category counts:")
print(h3_grouped["service_category"].value_counts())

# ------------ COMPUTE H3 CENTROID COORDINATES ------------
def h3_to_lat_lon(h):
    """Return (lat, lon) for an H3 cell, handling both old and new APIs."""
    try:
        # old API: h3.h3_to_geo(h)
        if hasattr(h3lib, "h3_to_geo"):
            return h3lib.h3_to_geo(h)
        # new API: h3.cell_to_latlng(h)
        if hasattr(h3lib, "cell_to_latlng"):
            return h3lib.cell_to_latlng(h)
    except Exception as e:
        # uncomment for debugging if needed
        # print("Error converting", h, "->", e)
        return None, None
    return None, None

# Apply conversion
lat_list = []
lon_list = []

for h in h3_grouped["h3_res8_id"]:
    lat, lon = h3_to_lat_lon(h)
    lat_list.append(lat)
    lon_list.append(lon)

h3_grouped["lat"] = lat_list
h3_grouped["lon"] = lon_list

# Drop any rows where we couldn't get coordinates
h3_grouped = h3_grouped.dropna(subset=["lat", "lon"])
print("Rows after H3 coordinate conversion:", len(h3_grouped))

# ------------ REORDER & SAVE ------------
h3_points = h3_grouped[[
    "county_fips",
    "h3_res8_id",
    "lat",
    "lon",
    "max_down",
    "max_up",
    "provider_count",
    "provider_names",
    "tech_types",        # <- NEW COLUMN with aggregated technology types
    "service_category",
]]

h3_points.to_csv(out_path, index=False)

print("\nH3 point dataset saved to:")
print(out_path)

import json

IN_PATH = r"H:\Broadband_Project_1\data\counties.geojson"   # your downloaded file
OUT_PATH = r"H:\Broadband_Project_1\data\ky_counties.geojson"

with open(IN_PATH, "r") as f:
    data = json.load(f)

features = data["features"]

# Keep only Kentucky (STATEFP = "21")
ky_features = [f for f in features if f["properties"].get("STATEFP") == "21"]

# Ensure GEOID exists (STATEFP + COUNTYFP)
for f in ky_features:
    props = f["properties"]
    if "GEOID" not in props:
        props["GEOID"] = props["STATEFP"] + props["COUNTYFP"]

# Build final GeoJSON
ky_geo = {
    "type": "FeatureCollection",
    "features": ky_features
}

with open(OUT_PATH, "w") as out:
    json.dump(ky_geo, out)

print("Saved:", OUT_PATH)
print("Kentucky counties:", len(ky_features))

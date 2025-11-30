import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from pathlib import Path
import json

# ==================================================
# CONFIG: locate DB relative to this file
# ==================================================
THIS_DIR = Path(__file__).resolve().parent      # e.g. .../analysis
PROJECT_ROOT = THIS_DIR.parent                  # repo root
DB_PATH = PROJECT_ROOT / "db" / "broadband_ky.db"

st.set_page_config(
    page_title="KY Broadband Analytics Dashboard",
    page_icon="📶",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ==================================================
# CUSTOM CSS (SaaS look)
# ==================================================
SAAS_CSS = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.stApp {
    background-color: #f5f7fb;
    font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
}

/* Header */
.top-header {
    background: #ffffff;
    padding: 1.2rem 1.8rem;
    border-radius: 18px;
    box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
    margin-bottom: 1.3rem;
    border: 1px solid #e5e7eb;
}

/* Metric cards */
.metric-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.1rem 1.3rem;
    box-shadow: 0 6px 14px rgba(15, 23, 42, 0.05);
    border: 1px solid #e5e7eb;
}

/* Section container */
.section-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.2rem 1.4rem;
    box-shadow: 0 6px 14px rgba(15, 23, 42, 0.04);
    border: 1px solid #e5e7eb;
    margin-bottom: 1rem;
}

/* Label above metrics */
.small-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    color: #6b7280;
    font-weight: 600;
    margin-bottom: 0.35rem;
}
</style>
"""
st.markdown(SAAS_CSS, unsafe_allow_html=True)

# ==================================================
# HELPERS
# ==================================================


def normalize_series(s: pd.Series) -> pd.Series:
    """Normalize a numeric series to 0–1; return 0.5 if constant/empty."""
    s = pd.to_numeric(s, errors="coerce")
    if s.empty:
        return pd.Series(0.5, index=s.index)
    minv = s.min()
    maxv = s.max()
    if pd.isna(minv) or pd.isna(maxv) or maxv == minv:
        return pd.Series(0.5, index=s.index)
    return (s - minv) / (maxv - minv)


@st.cache_data(show_spinner="Loading broadband database…")
def load_db():
    conn = sqlite3.connect(str(DB_PATH))

    county_df = pd.read_sql("SELECT * FROM county_summary", conn)
    provider_df = pd.read_sql("SELECT * FROM provider_summary_by_county", conn)
    hex_df = pd.read_sql("SELECT * FROM hex_coverage", conn)

    conn.close()

    # standardize county_fips to 5-char strings
    for df in (county_df, provider_df, hex_df):
        if "county_fips" in df.columns:
            df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    return county_df, provider_df, hex_df


@st.cache_data
def load_ky_county_geojson():
    """
    Load Kentucky county boundaries GeoJSON.

    Expected path: project_root/data/ky_counties.geojson
    Expected property: properties.GEOID = 5-digit county FIPS
    """
    gj_path = PROJECT_ROOT / "data" / "ky_counties.geojson"
    if not gj_path.exists():
        return None

    with open(gj_path, "r") as f:
        return json.load(f)
    
def enrich_county_with_hex(county_df: pd.DataFrame, hex_df: pd.DataFrame) -> pd.DataFrame:
    """Attach hex service-category counts and scores to county_df."""
    # hex counts per county by service_category
    svc_counts = (
        hex_df.groupby(["county_fips", "service_category"])
        .size()
        .unstack(fill_value=0)
    )

    # Ensure consistent columns
    for col in ["Unserved", "Underserved", "Served", "Unknown"]:
        if col not in svc_counts.columns:
            svc_counts[col] = 0

    svc_counts = svc_counts.reset_index().rename(
        columns={
            "Unserved": "hex_unserved",
            "Underserved": "hex_underserved",
            "Served": "hex_served",
            "Unknown": "hex_unknown",
        }
    )
    svc_counts["hex_total"] = (
        svc_counts["hex_unserved"]
        + svc_counts["hex_underserved"]
        + svc_counts["hex_served"]
        + svc_counts["hex_unknown"]
    )
    svc_counts["pct_unserved_hex"] = svc_counts["hex_unserved"] / svc_counts["hex_total"].replace(
        {0: pd.NA}
    )
    svc_counts["pct_underserved_hex"] = svc_counts["hex_underserved"] / svc_counts[
        "hex_total"
    ].replace({0: pd.NA})

    df = county_df.merge(svc_counts, on="county_fips", how="left")

    # Fill NaNs where appropriate
    for col in [
        "hex_unserved",
        "hex_underserved",
        "hex_served",
        "hex_unknown",
        "hex_total",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    for col in ["pct_unserved_hex", "pct_underserved_hex"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    # --------------------------------------------------
    # Compute Broadband Quality Score
    # --------------------------------------------------
    unserved_rate = df["pct_unserved_hex"].clip(0, 1)
    underserved_rate = df["pct_underserved_hex"].clip(0, 1)
    down_norm = normalize_series(df.get("county_avg_down", 0))
    prov_norm = normalize_series(df.get("provider_count", 0))

    # Higher is better: lower un/underserved, higher down speed, more providers
    df["broadband_quality_score"] = 100 * (
        (1 - unserved_rate) * 0.4
        + (1 - underserved_rate) * 0.2
        + down_norm * 0.25
        + prov_norm * 0.15
    )

    # --------------------------------------------------
    # Compute Digital Readiness Index
    # --------------------------------------------------
    edu_low = (
        pd.to_numeric(df.get("Less_Than_9th_grade", 0), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("Less_Than_HighSchool", 0), errors="coerce").fillna(0)
    )
    edu_high = pd.to_numeric(df.get("Atleast_Bachelors", 0), errors="coerce").fillna(0)
    edu_total = edu_low + edu_high
    edu_high_share = edu_high / edu_total.replace({0: pd.NA})
    edu_high_share = edu_high_share.fillna(0)

    population = pd.to_numeric(df.get("Population", 0), errors="coerce").fillna(0)
    desktop = pd.to_numeric(df.get("desktop_laptop_estimate", 0), errors="coerce").fillna(0)
    smartphone = pd.to_numeric(df.get("smartphone_estimate", 0), errors="coerce").fillna(0)
    devices_per_person = (desktop + smartphone) / population.replace({0: pd.NA})
    devices_per_person = devices_per_person.fillna(0)

    income = pd.to_numeric(
        df.get("Median_Household_Income", 0), errors="coerce"
    ).fillna(0)
    poverty = pd.to_numeric(df.get("total_est_poverty", 0), errors="coerce").fillna(0)
    poverty_rate = poverty / population.replace({0: pd.NA})
    poverty_rate = poverty_rate.fillna(0)
    poverty_comfort = 1 - poverty_rate  # higher is better

    edu_norm = normalize_series(edu_high_share)
    device_norm = normalize_series(devices_per_person)
    income_norm = normalize_series(income)
    poverty_norm = normalize_series(poverty_comfort)

    df["digital_readiness_index"] = 100 * (
        edu_norm * 0.35
        + device_norm * 0.25
        + income_norm * 0.25
        + poverty_norm * 0.15
    )

    return df


# ==================================================
# LOAD DATA
# ==================================================
    
county_df_raw, provider_df, hex_df = load_db()
county_df = enrich_county_with_hex(county_df_raw, hex_df)
ky_geojson = load_ky_county_geojson()

# Pre-calc lists for filters
service_categories = sorted(hex_df["service_category"].dropna().unique().tolist())
all_providers = sorted(provider_df["provider_name"].dropna().unique().tolist())
# Extract tech types list
tech_values = set()
for val in hex_df["tech_types"].dropna().unique():
    for t in str(val).split(";"):
        t = t.strip()
        if t:
            tech_values.add(t)
all_tech_types = sorted(tech_values)

# County labels
county_options = (
    county_df[["county_fips", "county_name"]]
    .drop_duplicates()
    .sort_values("county_name")
)
county_options["label"] = (
    county_options["county_name"] + " (" + county_options["county_fips"] + ")"
)

# ==================================================
# HEADER
# ==================================================
st.markdown('<div class="top-header">', unsafe_allow_html=True)
c1, c2, c3 = st.columns([0.8, 5, 2])

with c1:
    st.write("📶")

with c2:
    st.markdown("### Kentucky Broadband Analytics Dashboard")
    st.markdown(
        "Statewide and county-level analytics built from **FCC BDC**, "
        "**Census ACS**, and **H3 spatial aggregation**."
    )

with c3:
    st.caption("MSIS 695 – Capstone Project")
    st.caption("Data Store: SQLite · `broadband_ky.db`")

st.markdown("</div>", unsafe_allow_html=True)

# ==================================================
# TOP FILTER BAR
# ==================================================
fb1, fb2, fb3, fb4 = st.columns([1.7, 1.3, 1.6, 1.6])

# County filter
with fb1:
    county_labels = ["All Kentucky"] + county_options["label"].tolist()
    county_choice = st.selectbox("County", county_labels, index=0)

    if county_choice == "All Kentucky":
        selected_fips = None
        selected_county_name = "All Kentucky"
    else:
        row = county_options[county_options["label"] == county_choice].iloc[0]
        selected_fips = row["county_fips"]
        selected_county_name = row["county_name"]

# Service category filter
with fb2:
    svc_choices = ["All"] + service_categories
    svc_choice = st.selectbox("Service category", svc_choices, index=0)

# Provider filter
with fb3:
    provider_choices = ["All providers"] + all_providers
    provider_choice = st.selectbox("Provider", provider_choices, index=0)

# Tech type filter
with fb4:
    tech_choices = ["All technologies"] + all_tech_types
    tech_choice = st.selectbox("Tech type", tech_choices, index=0)

# ==================================================
# FILTERED DATASETS
# ==================================================
# County subset (for demographics & scores)
if selected_fips is None:
    scope_counties_df = county_df.copy()
else:
    scope_counties_df = county_df[county_df["county_fips"] == selected_fips].copy()

# Hex subset (for tech, map, service stats)
hex_filtered = hex_df.copy()
if selected_fips is not None:
    hex_filtered = hex_filtered[hex_filtered["county_fips"] == selected_fips]

if svc_choice != "All":
    hex_filtered = hex_filtered[hex_filtered["service_category"] == svc_choice]

if provider_choice != "All providers":
    hex_filtered = hex_filtered[
        hex_filtered["provider_names"].str.contains(provider_choice, na=False)
    ]

if tech_choice != "All technologies":
    hex_filtered = hex_filtered[
        hex_filtered["tech_types"].fillna("").str.contains(tech_choice, na=False)
    ]

# Provider subset for charts
prov_filtered = provider_df.copy()
if selected_fips is not None:
    prov_filtered = prov_filtered[prov_filtered["county_fips"] == selected_fips]
if provider_choice != "All providers":
    prov_filtered = prov_filtered[prov_filtered["provider_name"] == provider_choice]

# For KPI service totals we want **only county filter**, not service/provider/tech filters
if selected_fips is None:
    hex_for_kpi = hex_df.copy()
else:
    hex_for_kpi = hex_df[hex_df["county_fips"] == selected_fips]

# ==================================================
# HIGH-LEVEL KPIs
# ==================================================
# Demographic aggregates
pop_total = pd.to_numeric(scope_counties_df["Population"], errors="coerce").sum()
poverty_total = pd.to_numeric(
    scope_counties_df["total_est_poverty"], errors="coerce"
).sum()
area_total = pd.to_numeric(scope_counties_df["area_sq_mi"], errors="coerce").sum()
desktop_total = pd.to_numeric(
    scope_counties_df["desktop_laptop_estimate"], errors="coerce"
).sum()
smart_total = pd.to_numeric(
    scope_counties_df["smartphone_estimate"], errors="coerce"
).sum()

if pop_total > 0:
    poverty_rate_scope = poverty_total / pop_total
else:
    poverty_rate_scope = 0.0

# Statewide/service KPIs
unserved_total = (hex_for_kpi["service_category"] == "Unserved").sum()
underserved_total = (hex_for_kpi["service_category"] == "Underserved").sum()
served_total = (hex_for_kpi["service_category"] == "Served").sum()
hex_total_scope = len(hex_for_kpi)

# Scores
if selected_fips is None:
    # population-weighted average across counties
    w = pd.to_numeric(scope_counties_df["Population"], errors="coerce").fillna(0)
    bqs = (
        (scope_counties_df["broadband_quality_score"] * w).sum()
        / w.replace({0: pd.NA}).sum()
    )
    dri = (
        (scope_counties_df["digital_readiness_index"] * w).sum()
        / w.replace({0: pd.NA}).sum()
    )
else:
    row = scope_counties_df.iloc[0]
    bqs = float(row["broadband_quality_score"])
    dri = float(row["digital_readiness_index"])

# ==================================================
# TABS
# ==================================================
tab_overview, tab_explorer, tab_data = st.tabs(
    ["📌 Kentucky Overview", "🗺 County Explorer", "📄 Data & Rankings"]
)

# --------------------------------------------------
# TAB 1 – KENTUCKY OVERVIEW
# --------------------------------------------------
with tab_overview:
    st.markdown(
        f"#### Overview – **{selected_county_name}**"
        + ("" if svc_choice == "All" else f" · Service: **{svc_choice}**")
    )

    # KPI row 1: population & service coverage
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Total Population</div>', unsafe_allow_html=True
        )
        st.metric("", f"{int(pop_total):,}")
        st.markdown("</div>", unsafe_allow_html=True)

    with k2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Unserved hex cells (red)</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{int(unserved_total):,}")
        st.markdown("</div>", unsafe_allow_html=True)

    with k3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Underserved hex cells (yellow)</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{int(underserved_total):,}")
        st.markdown("</div>", unsafe_allow_html=True)

    with k4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Served hex cells (green)</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{int(served_total):,}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.write("")

    # KPI row 2: scores
    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Broadband Quality Score (0–100)</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{bqs:0.1f}")
        st.markdown("</div>", unsafe_allow_html=True)

    with s2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Digital Readiness Index (0–100)</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{dri:0.1f}")
        st.markdown("</div>", unsafe_allow_html=True)

    with s3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="small-label">Poverty Rate</div>',
            unsafe_allow_html=True,
        )
        st.metric("", f"{poverty_rate_scope*100:0.1f}%")
        st.markdown("</div>", unsafe_allow_html=True)

    st.write("")

    # === Technology vs Devices row ===
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Technology Mix & Device Access")

    c_left, c_right = st.columns([1.5, 1])

    # Tech mix pie
    with c_left:
        if hex_filtered.empty:
            st.info("No hex cells match the current filters.")
        else:
            tech_df = hex_filtered.copy()
            tech_df["tech_types"] = tech_df["tech_types"].fillna("Unknown")

            # split semicolon-separated tech_types into individual rows
            tech_long = tech_df.assign(
                tech=tech_df["tech_types"].str.split(";")
            ).explode("tech")
            tech_long["tech"] = tech_long["tech"].str.strip()
            tech_long = tech_long[tech_long["tech"] != ""]  # drop blanks just in case

            # clean, duplicate-free aggregation
            tech_counts = (
                tech_long.groupby("tech", as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )

            fig_tech = px.pie(
                tech_counts,
                names="tech",
                values="count",
                title="Share of hex cells by technology type",
            )
            st.plotly_chart(fig_tech, use_container_width=True)

    # Devices bar
    with c_right:
        dev_df = pd.DataFrame(
            {
                "device": ["Desktop / Laptop", "Smartphone"],
                "count": [int(desktop_total), int(smart_total)],
            }
        )
        fig_dev = px.bar(
            dev_df,
            x="device",
            y="count",
            text="count",
            title="Household device estimates",
        )
        fig_dev.update_layout(xaxis_title="", yaxis_title="Households")
        st.plotly_chart(fig_dev, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # === Education + Provider row ===
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Education & Provider Presence")

    e_left, e_right = st.columns([1, 1.4])

    with e_left:
        edu_low_9 = pd.to_numeric(
            scope_counties_df["Less_Than_9th_grade"], errors="coerce"
        ).sum()
        edu_low_hs = pd.to_numeric(
            scope_counties_df["Less_Than_HighSchool"], errors="coerce"
        ).sum()
        edu_high = pd.to_numeric(
            scope_counties_df["Atleast_Bachelors"], errors="coerce"
        ).sum()

        edu_df = pd.DataFrame(
            {
                "level": ["< 9th grade", "9–12 (no diploma)", "Bachelor's or higher"],
                "count": [edu_low_9, edu_low_hs, edu_high],
            }
        )

        fig_edu = px.pie(
            edu_df,
            names="level",
            values="count",
            title="Education attainment (approximate counts)",
        )
        st.plotly_chart(fig_edu, use_container_width=True)

    with e_right:
        if prov_filtered.empty:
            st.info("No provider records match the current filters.")
        else:
            tmp = prov_filtered.copy()

            # make sure numeric
            tmp["locations"] = pd.to_numeric(tmp["locations"], errors="coerce").fillna(0)
            tmp["underserved_locations"] = pd.to_numeric(
                tmp["underserved_locations"], errors="coerce"
            ).fillna(0)

            # how many providers to show
            top_n = st.slider(
                "Number of providers to show (by locations)",
                min_value=5,
                max_value=40,
                value=20,
                step=5,
                key="prov_top_n",
            )

            # aggregate across selected counties
            prov_agg = (
                tmp.groupby("provider_name", as_index=False)[
                    ["locations", "underserved_locations"]
                ]
                .sum()
                .sort_values("locations", ascending=False)
                .head(top_n)
            )

            # pretty labels like 1,234,567
            prov_agg["locations_label"] = (
                prov_agg["locations"].round(0).astype(int).map("{:,}".format)
            )

            fig_prov = px.bar(
                prov_agg,
                x="locations",
                y="provider_name",
                orientation="h",
                hover_data={
                    "locations": ":,",
                    "underserved_locations": ":,",
                    "provider_name": True,
                },
                title="Provider footprint (locations in scope)",
            )

            fig_prov.update_traces(
                text=prov_agg["locations_label"],
                textposition="outside",
                cliponaxis=False,
            )

            fig_prov.update_layout(
                xaxis_title="Reported service locations",
                yaxis_title="Provider",
                margin=dict(l=0, r=20, t=60, b=40),
            )

            st.plotly_chart(fig_prov, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # === Rankings section (only meaningful for All Kentucky) ===
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("County Rankings – Broadband & Digital Readiness")

    if selected_fips is None:
        rank_df = (
            county_df[["county_name", "broadband_quality_score", "digital_readiness_index"]]
            .dropna()
            .copy()
        )
        top_broadband = rank_df.sort_values(
            "broadband_quality_score", ascending=False
        ).head(10)
        top_digital = rank_df.sort_values(
            "digital_readiness_index", ascending=False
        ).head(10)

        r1, r2 = st.columns(2)

        with r1:
            st.markdown("**Top 10 counties by Broadband Quality Score**")
            fig_bq = px.bar(
                top_broadband,
                x="broadband_quality_score",
                y="county_name",
                orientation="h",
                range_x=[0, 100],
                labels={"broadband_quality_score": "Score"},
            )
            st.plotly_chart(fig_bq, use_container_width=True)

        with r2:
            st.markdown("**Top 10 counties by Digital Readiness Index**")
            fig_dr = px.bar(
                top_digital,
                x="digital_readiness_index",
                y="county_name",
                orientation="h",
                range_x=[0, 100],
                labels={"digital_readiness_index": "Index"},
            )
            st.plotly_chart(fig_dr, use_container_width=True)
    else:
        st.info(
            "Rankings are most informative at the statewide level. "
            "Switch county filter to **All Kentucky** to see rankings."
        )

    st.markdown("</div>", unsafe_allow_html=True)

# --------------------------------------------------
# TAB 2 – COUNTY EXPLORER (MAP + LOCAL DETAIL)
# --------------------------------------------------
with tab_explorer:
    st.markdown("#### County Explorer")

    # IF ALL KENTUCKY -> show statewide county choropleth
    if selected_fips is None:
        if ky_geojson is None:
            st.warning(
                "Kentucky county GeoJSON not found at `data/ky_counties.geojson`. "
                "Add it to the repo to see the statewide map."
            )
        else:
            # ---------- BUILD MAP DATAFRAME DEPENDING ON PROVIDER FILTER ----------
            if provider_choice == "All providers":
                st.markdown(
                    "Exploring **All Kentucky** – county-level broadband coverage "
                    "based on FCC BDC hex aggregation (all providers)."
                )

                df_map = county_df.copy()

                metric_options = {
                    "Percent unserved hex cells": "pct_unserved_hex",
                    "Percent underserved hex cells": "pct_underserved_hex",
                    "Broadband Quality Score (0–100)": "broadband_quality_score",
                    "Digital Readiness Index (0–100)": "digital_readiness_index",
                }

            else:
                st.markdown(
                    f"Exploring **All Kentucky** – footprint for provider "
                    f"**{provider_choice}**."
                )

                tmp = provider_df[provider_df["provider_name"] == provider_choice].copy()
                tmp["locations"] = pd.to_numeric(tmp["locations"], errors="coerce").fillna(0)
                tmp["underserved_locations"] = pd.to_numeric(
                    tmp["underserved_locations"], errors="coerce"
                ).fillna(0)

                # aggregate provider metrics per county
                prov_agg = (
                    tmp.groupby("county_fips", as_index=False)[
                        ["locations", "underserved_locations"]
                    ]
                    .sum()
                )

                # base county info (including total_locations for share)
                base_cols = [
                    c
                    for c in [
                        "county_fips",
                        "county_name",
                        "total_locations",
                        "broadband_quality_score",
                        "digital_readiness_index",
                        "pct_unserved_hex",
                        "pct_underserved_hex",
                    ]
                    if c in county_df.columns
                ]
                df_map = county_df[base_cols].merge(
                    prov_agg, on="county_fips", how="left"
                )

                df_map[["locations", "underserved_locations"]] = df_map[
                    ["locations", "underserved_locations"]
                ].fillna(0)

                # provider's share of locations in each county (0–100%)
                if "total_locations" in df_map.columns:
                    denom = pd.to_numeric(
                        df_map["total_locations"], errors="coerce"
                    ).replace({0: pd.NA})
                    df_map["provider_coverage_share"] = (
                        df_map["locations"] / denom
                    ) * 100
                else:
                    df_map["provider_coverage_share"] = pd.NA

                df_map["provider_coverage_share"] = df_map[
                    "provider_coverage_share"
                ].fillna(0)

                metric_options = {
                    "Provider locations in county": "locations",
                    "Provider underserved locations": "underserved_locations",
                    "Provider coverage share (%)": "provider_coverage_share",
                }

            # ---------- SELECT METRIC TO COLOR BY ----------
            pretty_to_col = {
                label: col for label, col in metric_options.items() if col in df_map.columns
            }

            metric_label = st.selectbox(
                "Color counties by",
                list(pretty_to_col.keys()),
                index=0,
            )
            metric_col = pretty_to_col[metric_label]

            # if it's a fraction, convert to 0–100 for legend readability
            if metric_col in ["pct_unserved_hex", "pct_underserved_hex"]:
                df_map[metric_col] = df_map[metric_col] * 100

            # ---------- BUILD SAFE HOVER DATA ----------
            hover_candidates = {
                "county_fips": True,
                "locations": ":,",
                "underserved_locations": ":,",
                "total_locations": ":,",
                "broadband_quality_score": True,
                "digital_readiness_index": True,
                "pct_unserved_hex": True,
                "pct_underserved_hex": True,
                "provider_coverage_share": True,
            }
            hover_data = {
                col: spec for col, spec in hover_candidates.items() if col in df_map.columns
            }

            # ---------- DRAW CHOROPLETH ----------
            fig_state = px.choropleth_mapbox(
                df_map,
                geojson=ky_geojson,
                locations="county_fips",
                featureidkey="properties.GEOID",
                color=metric_col,
                hover_name="county_name",
                hover_data=hover_data,
                mapbox_style="carto-positron",
                center={"lat": 37.8, "lon": -85.8},
                zoom=6.2,
                opacity=0.85,
                height=650,
                color_continuous_scale="RdYlGn_r",
            )
            fig_state.update_layout(margin={"r": 0, "t": 10, "l": 0, "b": 0})

            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Statewide county-level broadband map")
            st.plotly_chart(fig_state, use_container_width=True)

            if provider_choice == "All providers":
                st.caption(
                    "Each polygon is a Kentucky county. Colors show the selected broadband "
                    "metric (e.g., percent unserved hex cells or broadband quality score) "
                    "aggregated across all providers."
                )
            else:
                st.caption(
                    f"Each polygon is a Kentucky county. Colors show **{provider_choice}**’s "
                    "footprint (locations, underserved locations, or coverage share) in each county."
                )

            st.info(
                "To see detailed H3 hex coverage for a specific county, choose a county "
                "in the **County** filter above."
            )

    # ELSE -> per-county hex map
    else:
        # Filter hexes for this county only, but keep provider/tech filters
        county_hex = hex_df[hex_df["county_fips"] == selected_fips].copy()

        if svc_choice != "All":
            county_hex = county_hex[
                county_hex["service_category"] == svc_choice
            ]
        if provider_choice != "All providers":
            county_hex = county_hex[
                county_hex["provider_names"].str.contains(provider_choice, na=False)
            ]
        if tech_choice != "All technologies":
            county_hex = county_hex[
                county_hex["tech_types"].fillna("").str.contains(tech_choice, na=False)
            ]

        total_points = len(county_hex)
        max_points = st.slider(
            "Max hex points to plot (for performance)",
            min_value=2000,
            max_value=70000,
            value=20000,
            step=2000,
        )

        if total_points == 0:
            st.warning("No hex cells match filters for this county.")
        else:
            if total_points > max_points:
                map_df = county_hex.sample(max_points, random_state=42)
            else:
                map_df = county_hex

            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Hex-level broadband map")

            if not {"lat", "lon"}.issubset(map_df.columns):
                st.error("Hex dataset is missing lat/lon columns.")
            else:
                color_map = {
                    "Unserved": "red",
                    "Underserved": "orange",
                    "Served": "green",
                    "Unknown": "gray",
                }
                fig_map = px.scatter_mapbox(
                    map_df,
                    lat="lat",
                    lon="lon",
                    color="service_category",
                    color_discrete_map=color_map,
                    zoom=8,
                    height=650,
                    hover_data={
                        "county_fips": True,
                        "max_down": True,
                        "max_up": True,
                        "provider_count": True,
                        "provider_names": True,
                        "tech_types": True,
                        "service_category": True,
                    },
                )
                fig_map.update_layout(
                    mapbox_style="open-street-map",
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},
                )
                st.plotly_chart(fig_map, use_container_width=True)

            st.caption(
                "Each point is an H3 hex cell with at least one broadband service report. "
                "Colors show FCC BDC service category; hover to see provider and technology details."
            )
            st.markdown("</div>", unsafe_allow_html=True)

            # Category breakdown for this county
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Service category breakdown in selected county")

            cat_counts = (
                county_hex["service_category"]
                .value_counts()
                .reindex(service_categories)
                .fillna(0)
                .astype(int)
                .reset_index()
            )
            cat_counts.columns = ["service_category", "count"]

            bc1, bc2 = st.columns([2, 1])

            with bc1:
                fig_bar = px.bar(
                    cat_counts,
                    x="service_category",
                    y="count",
                    color="service_category",
                    title="Hex cells by service category",
                    text="count",
                )
                fig_bar.update_layout(showlegend=False)
                st.plotly_chart(fig_bar, use_container_width=True)

            with bc2:
                fig_pie_cat = px.pie(
                    cat_counts,
                    names="service_category",
                    values="count",
                    title="Share of hex cells",
                )
                st.plotly_chart(fig_pie_cat, use_container_width=True)

            st.markdown("</div>", unsafe_allow_html=True)

# --------------------------------------------------
# TAB 3 – DATA & RANKINGS
# --------------------------------------------------
with tab_data:
    st.markdown("#### Data & County Scores")

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("County-level dataset (with scores)")

    display_cols = [
        "county_fips",
        "county_name",
        "county_avg_down",
        "total_locations",
        "hex_unserved",
        "hex_underserved",
        "hex_served",
        "broadband_quality_score",
        "digital_readiness_index",
        "Population",
        "Median_Household_Income",
        "desktop_laptop_estimate",
        "smartphone_estimate",
    ]
    display_cols = [c for c in display_cols if c in county_df.columns]

    st.dataframe(
        county_df[display_cols].sort_values("county_name"),
        use_container_width=True,
        height=450,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Raw hex data (filtered)")

    st.dataframe(
        hex_filtered.head(300),
        use_container_width=True,
        height=450,
    )
    st.markdown("</div>", unsafe_allow_html=True)

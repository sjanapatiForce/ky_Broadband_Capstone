import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# =========================================================
# CONFIG
# =========================================================
DB_PATH = r"db\broadband_ky.db"

st.set_page_config(
    page_title="KY Broadband Analytics Portal",
    page_icon="📶",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# CUSTOM SAAS UI CSS
# =========================================================
SAAS_STYLE = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.stApp {
    background-color: #f5f7fb;
}

/* Header card */
.top-header {
    background: #ffffff;
    padding: 1.2rem 1.8rem;
    border-radius: 18px;
    box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    border: 1px solid #e5e7eb;
    margin-bottom: 1.2rem;
}

/* Metric cards */
.metric-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.3rem;
    box-shadow: 0 6px 14px rgba(15,23,42,0.05);
    border: 1px solid #e5e7eb;
}

/* Section content cards */
.section-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.6rem;
    box-shadow: 0 6px 14px rgba(15,23,42,0.04);
    border: 1px solid #e5e7eb;
    margin-bottom: 1rem;
}

/* Labels */
.small-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6b7280;
    margin-bottom: 0.35rem;
    font-weight: 600;
}
</style>
"""
st.markdown(SAAS_STYLE, unsafe_allow_html=True)


# =========================================================
# LOAD DATA FROM SQLITE
# =========================================================
@st.cache_data(show_spinner="Loading broadband database…")
def load_db():
    conn = sqlite3.connect(DB_PATH)

    county_df = pd.read_sql("SELECT * FROM county_summary", conn)
    provider_df = pd.read_sql("SELECT * FROM provider_summary_by_county", conn)
    hex_df = pd.read_sql("""
        SELECT h.*, c.county_name
        FROM hex_coverage h
        LEFT JOIN county_summary c
        ON h.county_fips = c.county_fips
    """, conn)

    conn.close()

    # normalize formats
    county_df["county_fips"] = county_df["county_fips"].astype(str).str.zfill(5)
    provider_df["county_fips"] = provider_df["county_fips"].astype(str).str.zfill(5)
    hex_df["county_fips"] = hex_df["county_fips"].astype(str).str.zfill(5)

    return county_df, provider_df, hex_df


county_df, provider_df, hex_df = load_db()


# =========================================================
# HEADER
# =========================================================
st.markdown('<div class="top-header">', unsafe_allow_html=True)
col1, col2 = st.columns([1, 7])

with col1:
    st.write("📶")

with col2:
    st.markdown("### Kentucky Broadband Analytics Portal")
    st.markdown(
        "Interactive dashboard powered by **FCC BDC** + **U.S. Census** + **H3 hex coverage**. "
        "Identify digital gaps, underserved regions, and provider performance."
    )

st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# SIDEBAR FILTERS
# =========================================================
st.sidebar.header("📊 Filters")

county_opts = (
    county_df[["county_fips", "county_name"]]
    .drop_duplicates()
    .sort_values("county_name")
)
county_opts["label"] = county_opts["county_name"] + " (" + county_opts["county_fips"] + ")"

selected_label = st.sidebar.selectbox(
    "Choose a County",
    county_opts["label"].tolist()
)

selected_fips = county_opts.loc[county_opts["label"] == selected_label, "county_fips"].iloc[0]
selected_county = county_opts.loc[county_opts["label"] == selected_label, "county_name"].iloc[0]

service_categories = sorted(hex_df["service_category"].dropna().unique())
default_categories = [c for c in ["Unserved", "Underserved", "Served"] if c in service_categories]

selected_categories = st.sidebar.multiselect(
    "Service categories",
    options=service_categories,
    default=default_categories
)

max_points = st.sidebar.slider(
    "Max hex points for map",
    min_value=2000,
    max_value=70000,
    value=20000,
    step=2000
)


# =========================================================
# FILTER DATA FOR SELECTED COUNTY
# =========================================================
county_hex = hex_df[hex_df["county_fips"] == selected_fips].copy()
county_row = county_df[county_df["county_fips"] == selected_fips].iloc[0]
county_providers = provider_df[provider_df["county_fips"] == selected_fips].copy()

if selected_categories:
    county_hex = county_hex[county_hex["service_category"].isin(selected_categories)]

total_hex = len(county_hex)


# =========================================================
# KPI CARDS
# =========================================================
st.markdown(f"#### County Overview: **{selected_county} ({selected_fips})**")

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Hex Cells (Filtered)</div>', unsafe_allow_html=True)
    st.metric("", f"{total_hex:,}")
    st.markdown('</div>', unsafe_allow_html=True)

with k2:
    unserved = (county_hex["service_category"] == "Unserved").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Unserved (&lt;25/3)</div>', unsafe_allow_html=True)
    st.metric("", f"{unserved:,}")
    st.markdown('</div>', unsafe_allow_html=True)

with k3:
    underserved = (county_hex["service_category"] == "Underserved").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Underserved (&lt;100/20)</div>', unsafe_allow_html=True)
    st.metric("", f"{underserved:,}")
    st.markdown('</div>', unsafe_allow_html=True)

with k4:
    served = (county_hex["service_category"] == "Served").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Served</div>', unsafe_allow_html=True)
    st.metric("", f"{served:,}")
    st.markdown('</div>', unsafe_allow_html=True)


# =========================================================
# TABS
# =========================================================
tab_overview, tab_map, tab_tech, tab_profile, tab_data = st.tabs(
    ["📌 Overview", "🗺 Map", "🛰 Tech & Providers", "🏛 County Profile", "📄 Data"]
)


# =========================================================
# TAB: OVERVIEW
# =========================================================
with tab_overview:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Service Category Breakdown")

    cat_counts = (
        hex_df[hex_df["county_fips"] == selected_fips]["service_category"]
        .value_counts()
        .reindex(service_categories)
        .fillna(0)
        .reset_index()
    )
    cat_counts.columns = ["service_category", "count"]

    fig = px.bar(
        cat_counts,
        x="service_category",
        y="count",
        text="count",
        color="service_category",
        title="Hex Cells by Service Category",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB: MAP
# =========================================================
with tab_map:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Hex-Level Broadband Map")

    if len(county_hex) > max_points:
        map_df = county_hex.sample(max_points, random_state=42)
    else:
        map_df = county_hex

    if "lat" in map_df.columns and "lon" in map_df.columns:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat",
            lon="lon",
            color="service_category",
            zoom=8,
            height=650,
            color_discrete_map={
                "Unserved": "red",
                "Underserved": "orange",
                "Served": "green",
                "Unknown": "gray",
            },
            hover_data={
                "max_down": True,
                "max_up": True,
                "provider_count": True,
                "provider_names": True,
                "tech_types": True,
                "lat": False,
                "lon": False,
            }
        )
        fig_map.update_layout(mapbox_style="open-street-map")
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.error("Latitude/Longitude not available for this county.")

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB: TECH & PROVIDERS
# =========================================================
with tab_tech:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Technology Mix & Provider Performance")

    colA, colB = st.columns([1.2, 1.2])

    # Technology
    with colA:
        st.markdown("**Technology Mix (Based on Hex Cells)**")

        tech_df = county_hex.copy()
        tech_df["tech_types"] = tech_df["tech_types"].fillna("Unknown")
        tech_long = tech_df.assign(tech=tech_df["tech_types"].str.split(";")).explode("tech")
        tech_long["tech"] = tech_long["tech"].str.strip()

        tech_counts = tech_long["tech"].value_counts().reset_index()
        tech_counts.columns = ["tech", "count"]

        fig_tech = px.pie(
            tech_counts,
            names="tech",
            values="count",
            title="Technology Share",
        )
        st.plotly_chart(fig_tech, use_container_width=True)

    # Providers
    with colB:
        st.markdown("**Provider Performance**")
        if not county_providers.empty:
            fig_prov = px.bar(
                county_providers,
                x="provider_name",
                y="avg_down",
                title="Average Downstream Speed by Provider (Mbps)",
                hover_data=["avg_up", "locations", "underserved_locations"]
            )
            fig_prov.update_layout(xaxis_title="Provider", yaxis_title="Avg Downstream (Mbps)")
            st.plotly_chart(fig_prov, use_container_width=True)
        else:
            st.info("No provider data available.")

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB: COUNTY PROFILE
# =========================================================
with tab_profile:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("County Demographics & Device Access")

    c1, c2 = st.columns([1.2, 1.2])

    with c1:
        st.markdown("### Demographics")
        st.write(f"- **Population**: {int(county_row['Population']):,}")
        st.write(f"- **Poverty Estimate**: {int(county_row['total_est_poverty']):,}")
        st.write(f"- **Median Household Income**: ${county_row['Median_Household_Income']:,}")
        st.write(f"- **Area**: {county_row['area_sq_mi']:.1f} sq. miles")

        st.markdown("### Education Levels")
        st.write(f"- Less Than 9th Grade: {int(county_row['Less_Than_9th_grade']):,}")
        st.write(f"- Less Than High School: {int(county_row['Less_Than_HighSchool']):,}")
        st.write(f"- Bachelor's or Above: {int(county_row['Atleast_Bachelors']):,}")

    with c2:
        st.markdown("### Device Access")
        device_df = pd.DataFrame({
            "Device": ["Desktop/Laptop", "Smartphone"],
            "Estimate": [
                int(county_row["desktop_laptop_estimate"]),
                int(county_row["smartphone_estimate"])
            ]
        })
        fig_dev = px.bar(device_df, x="Device", y="Estimate", text="Estimate",
                         title="Device Access (Estimated Households)")
        st.plotly_chart(fig_dev, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# TAB: DATA EXPLORER
# =========================================================
with tab_data:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Data Explorer")

    st.markdown("### Hex-Level Data (filtered)")
    st.dataframe(
        county_hex[
            [
                "h3_res8_id", "service_category",
                "max_down", "max_up",
                "provider_count", "provider_names",
                "tech_types", "lat", "lon"
            ]
        ].head(200),
        use_container_width=True
    )

    st.markdown("### County Summary Row")
    st.dataframe(county_df[county_df["county_fips"] == selected_fips], use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

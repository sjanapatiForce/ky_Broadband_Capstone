import streamlit as st
import pandas as pd
import plotly.express as px

# -----------------------------
# FILE PATHS
# -----------------------------
PATH_BLOCK = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\bdc_h3_points.csv"
PATH_COUNTY = r"H:\Broadband_Project_1\datasets\bdc_data_1\final\ky_bdc_final_dataset.csv"

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="KY Broadband Block Explorer",
    page_icon="📶",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# CUSTOM CSS FOR SAAS LOOK
# -----------------------------
SAAS_CSS = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.stApp {
    background-color: #f5f7fb;
}

/* Top header card */
.top-header {
    background: #ffffff;
    padding: 1.2rem 1.8rem;
    border-radius: 18px;
    box-shadow: 0 8px 18px rgba(15, 23, 42, 0.06);
    margin-bottom: 1.2rem;
}

/* Metric cards */
.metric-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.1rem 1.3rem;
    box-shadow: 0 6px 14px rgba(15, 23, 42, 0.05);
    border: 1px solid #e5e7eb;
}

/* Section content cards */
.section-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 1.2rem 1.4rem;
    box-shadow: 0 6px 14px rgba(15, 23, 42, 0.04);
}

/* Titles */
h1, h2, h3, h4 {
    font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
}

/* Small label above metrics */
.small-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6b7280;
    font-weight: 600;
    margin-bottom: 0.35rem;
}
</style>
"""
st.markdown(SAAS_CSS, unsafe_allow_html=True)

# -----------------------------
# TOP HEADER
# -----------------------------
st.markdown('<div class="top-header">', unsafe_allow_html=True)

col_logo, col_title, col_right = st.columns([0.8, 4, 2])

with col_logo:
    st.write("📶")

with col_title:
    st.markdown("### Kentucky Broadband Coverage – Block-Level Explorer")
    st.markdown(
        "Explore **block-level broadband availability** across Kentucky counties. "
        "Filter by service category to identify unserved and underserved areas for grant justification."
    )

with col_right:
    st.caption("MSIS 695 • Capstone Dashboard")
    st.caption("Data: FCC BDC + County metadata")

st.markdown("</div>", unsafe_allow_html=True)

st.write("")

# -----------------------------
# LOAD DATA
# -----------------------------
with st.spinner("Loading block-level dataset..."):
    try:
        block_df = pd.read_csv(PATH_BLOCK)
    except Exception as e:
        st.error(f"Error loading block-level file: {e}")
        st.stop()

with st.spinner("Loading county dataset (if available)..."):
    try:
        county_df = pd.read_csv(PATH_COUNTY, dtype={"county_fips": str})
        county_df["county_fips"] = county_df["county_fips"].astype(str).str.zfill(5)
    except Exception:
        st.warning("County dataset not found or could not be loaded.")
        county_df = pd.DataFrame()

# Basic data cleanup
if "county_fips" not in block_df.columns:
    st.error("Dataset missing county_fips column.")
    st.stop()

block_df["county_fips"] = block_df["county_fips"].astype(str).str.zfill(5)

if "county_name" in county_df.columns:
    block_df = block_df.merge(
        county_df[["county_fips", "county_name"]].drop_duplicates(),
        on="county_fips",
        how="left",
    )
else:
    block_df["county_name"] = block_df["county_fips"]

# -----------------------------
# DATA PREVIEW (IN EXPANDERS)
# -----------------------------
with st.expander("Preview block-level dataset (first 5 rows)", expanded=False):
    st.write(block_df.head())
    st.write("Columns:", list(block_df.columns))

if not county_df.empty:
    with st.expander("Preview county dataset (first 5 rows)", expanded=False):
        st.write(county_df.head())
        st.write("Columns:", list(county_df.columns))

# -----------------------------
# SIDEBAR FILTERS
# -----------------------------
st.sidebar.header("📊 Filters")

county_options = (
    block_df[["county_fips", "county_name"]]
    .drop_duplicates()
    .sort_values("county_name")
)
county_options["label"] = county_options["county_name"] + " (" + county_options["county_fips"] + ")"

selected_label = st.sidebar.selectbox(
    "Select County",
    options=county_options["label"].tolist(),
)

selected_fips = county_options.loc[
    county_options["label"] == selected_label, "county_fips"
].iloc[0]

service_categories = sorted(block_df["service_category"].dropna().unique().tolist())
default_cats = [c for c in ["Unserved", "Underserved", "Served"] if c in service_categories]

selected_categories = st.sidebar.multiselect(
    "Service categories",
    options=service_categories,
    default=default_cats,
)

max_points = st.sidebar.slider(
    "Max block points to plot (for performance)",
    min_value=1000,
    max_value=60000,
    value=20000,
    step=1000,
)

st.sidebar.caption(
    "Tip: Focus on **Unserved** and **Underserved** to identify grant-eligible areas."
)

# -----------------------------
# FILTER BY COUNTY & CATEGORY
# -----------------------------
county_blocks = block_df[block_df["county_fips"] == selected_fips].copy()

if selected_categories:
    county_blocks = county_blocks[county_blocks["service_category"].isin(selected_categories)]

total_points = len(county_blocks)

if total_points == 0:
    st.warning("No block areas match the current filters for this county.")
    st.stop()

if total_points > max_points:
    county_blocks_sample = county_blocks.sample(max_points, random_state=42)
else:
    county_blocks_sample = county_blocks

# -----------------------------
# KPI CARDS ROW
# -----------------------------
county_name_display = county_blocks_sample["county_name"].iloc[0]

st.markdown(
    f"###### County Overview: **{county_name_display} ({selected_fips})**"
)

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

with kpi_col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Block Areas (Total)</div>', unsafe_allow_html=True)
    st.metric(label="", value=f"{total_points:,}")
    st.markdown("</div>", unsafe_allow_html=True)

with kpi_col2:
    unserved = (county_blocks["service_category"] == "Unserved").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Unserved Blocks (&lt;25/3)</div>', unsafe_allow_html=True)
    st.metric(label="", value=f"{unserved:,}")
    st.markdown("</div>", unsafe_allow_html=True)

with kpi_col3:
    underserved = (county_blocks["service_category"] == "Underserved").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Underserved Blocks (&lt;100/20)</div>', unsafe_allow_html=True)
    st.metric(label="", value=f"{underserved:,}")
    st.markdown("</div>", unsafe_allow_html=True)

with kpi_col4:
    served = (county_blocks["service_category"] == "Served").sum()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-label">Served Blocks</div>', unsafe_allow_html=True)
    st.metric(label="", value=f"{served:,}")
    st.markdown("</div>", unsafe_allow_html=True)

st.write("")

# -----------------------------
# MAIN TABS
# -----------------------------
tab_map, tab_breakdown, tab_data = st.tabs(
    ["🗺 Map", "📈 Category Breakdown", "📄 Data"]
)

# -----------------------------
# MAP TAB
# -----------------------------
with tab_map:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Block-Level Broadband Map")

    if not {"lat", "lon"}.issubset(county_blocks_sample.columns):
        st.error("Dataset missing lat/lon columns.")
    else:
        color_map = {
            "Unserved": "red",
            "Underserved": "orange",
            "Served": "green",
            "Unknown": "gray",
        }

        fig = px.scatter_mapbox(
            county_blocks_sample,
            lat="lat",
            lon="lon",
            color="service_category",
            color_discrete_map=color_map,
            hover_data={
                "county_fips": True,
                "max_down": True,
                "max_up": True,
                "provider_count": True,
                "provider_names": True,
                "service_category": True,
                "lat": False,
                "lon": False,
            },
            zoom=8,
            height=650,
        )

        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=0.01,
                xanchor="left",
                x=0.01,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Each point represents a reported broadband service location within the selected county. "
            "Colors show service category: red = unserved, orange = underserved, green = served."
        )

    st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# CATEGORY BREAKDOWN TAB
# -----------------------------
with tab_breakdown:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Block Category Breakdown")

    # DEBUG (temporary): show columns so we know what exists
    # st.write("Columns in county_blocks:", list(county_blocks.columns))

    # ---- Existing category breakdown ----
    cat_counts = (
        county_blocks["service_category"]
        .value_counts()
        .reindex(service_categories)
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    cat_counts.columns = ["service_category", "count"]

    c1, c2 = st.columns([2, 1])

    with c1:
        fig_bar = px.bar(
            cat_counts,
            x="service_category",
            y="count",
            color="service_category",
            color_discrete_map={
                "Unserved": "red",
                "Underserved": "orange",
                "Served": "green",
                "Unknown": "gray",
            },
            text="count",
            title="Block areas by category",
        )
        fig_bar.update_layout(showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        fig_pie = px.pie(
            cat_counts,
            names="service_category",
            values="count",
            color="service_category",
            color_discrete_map={
                "Unserved": "red",
                "Underserved": "orange",
                "Served": "green",
                "Unknown": "gray",
            },
            title="Share of block areas",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    st.caption(
        "Use this breakdown to quickly explain the proportion of unserved vs. underserved vs. served blocks in the selected county."
    )

    # ---- New section: Technology & Providers ----
    st.markdown("---")
    st.subheader("Technology Mix & Provider Presence")

    tech_col, prov_col = st.columns([1, 1])

    # =======================
    # TECH TYPE PIE CHART
    # =======================
    with tech_col:
        if "tech_types" not in county_blocks.columns:
            st.info("Column 'tech_types' not found in dataset. Check that your bdc_h3_points.csv includes this field.")
        else:
            tech_series = county_blocks["tech_types"].dropna()

            tech_long = (
                tech_series
                .astype(str)
                .str.split(";", expand=False)
                .explode()
                .str.strip()
            )
            tech_long = tech_long[tech_long != ""]

            if tech_long.empty:
                st.info("No technology type information available for this county.")
            else:
                tech_counts = tech_long.value_counts().reset_index()
                tech_counts.columns = ["tech_type", "count"]
                tech_counts["percentage"] = (
                    tech_counts["count"] / tech_counts["count"].sum() * 100
                ).round(1)

                fig_tech = px.pie(
                    tech_counts,
                    names="tech_type",
                    values="count",
                    title="Technology mix in this county",
                    hole=0.3,
                )
                st.plotly_chart(fig_tech, use_container_width=True)

                st.caption(
                    "Share of technology types across H3 hex cells in the selected county. "
                    "If a hex uses multiple technologies, each tech is counted once."
                )

    # =======================
    # PROVIDER BAR CHART
    # =======================
    with prov_col:
        if "provider_names" not in county_blocks.columns:
            st.info("Column 'provider_names' not found in dataset. Check that your bdc_h3_points.csv includes this field.")
        else:
            prov_series = county_blocks["provider_names"].dropna()

            provider_long = (
                prov_series
                .astype(str)
                .str.split(";", expand=False)
                .explode()
                .str.strip()
            )
            provider_long = provider_long[provider_long != ""]

            if provider_long.empty:
                st.info("No provider information available for this county.")
            else:
                provider_counts = provider_long.value_counts().reset_index()
                provider_counts.columns = ["provider_name", "hex_count"]

                # Top 15 providers to keep chart readable
                provider_counts_top = provider_counts.head(15)

                fig_prov = px.bar(
                    provider_counts_top,
                    x="hex_count",
                    y="provider_name",
                    orientation="h",
                    title="Hex coverage by provider (top 15)",
                    text="hex_count",
                )
                fig_prov.update_layout(yaxis=dict(automargin=True))
                st.plotly_chart(fig_prov, use_container_width=True)

                st.caption(
                    "Number of H3 hex cells where each provider reports service in the selected county. "
                    "Providers appearing in more hexes generally have wider coverage."
                )

    st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# DATA TAB
# -----------------------------
with tab_data:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Block Data Sample")

    st.dataframe(county_blocks.head(50), use_container_width=True)

    st.caption(
        "This is a sample of the filtered block-level data for the selected county. "
        "You can export the full dataset from your preprocessing pipeline for deeper analysis in Python or Power BI."
    )

    st.markdown("</div>", unsafe_allow_html=True)

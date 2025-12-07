# Broadband Project 
## Kentucky Broadband Analytics Dashboard
Description

This project provides a custom dashboard to help Kentucky counties analyze broadband coverage using FCC BDC data. It integrates FCC broadband availability data with hex-cell (H3) mapping to identify unserved and underserved areas, and presents interactive maps, charts, and tables for county-level and statewide analysis.

Features / Functionality

Statewide summary view (total population, counts of unserved / underserved / served hex cells & locations)

County-level drill-down with dynamic update of metrics, map, and charts

Interactive H3 hex coverage map with service classification coloring

Provider and technology-type breakdown charts for each county

County comparison & ranking table covering all Kentucky counties

Clean data processing pipeline (ETL) to clean raw FCC BDC input, generate hex cells, and store processed data in SQLite

Prerequisites / Requirements

Python 3.9 or newer

Required Python packages: streamlit, pandas, geopandas, h3, plotly, shapely, sqlite3 (or whichever DB driver used)

Moderate hardware (8 GB RAM or higher recommended) for processing data

Setup & Installation

Clone the repository:

git clone https://github.com/sjanapatiForce/ky_Broadband_Capstone.git
cd ky_Broadband_Capstone


Install dependencies:

pip install -r requirements.txt


(If you don’t have a requirements.txt, install manually: pip install streamlit pandas geopandas h3 plotly shapely sqlite3)

Prepare raw data: place downloaded FCC BDC CSV files for Kentucky (or selected counties) into the data_raw/ folder.

Run the ETL script to process data and build the database:

python src/etl/process_fcc_data.py


Launch the dashboard:

streamlit run app.py

Usage

On launching the dashboard you’ll land on the Statewide Overview page showing summary metrics.

Use dropdowns / filters to select a county, provider, service category, or technology type.

Explore interactive hex maps, provider/technology breakdowns, and county ranking tables.

(Optional) Export data or screenshots for reports or BEAD documentation.

Project Structure
ky_Broadband_Capstone/
│── data_raw/           # raw downloaded FCC CSVs
│── data_clean/         # cleaned data (optional)
│── database/           # SQLite database (broadband_ky.db)
│── src/
│   ├── etl/            # data cleaning and processing scripts
│   ├── mapping/        # geospatial logic, H3 hex assignment, classification
│   └── dashboard/      # Streamlit app modules
│── app.py              # main dashboard entry point
│── requirements.txt    # dependencies list
│── README.md           # this file

Notes & Limitations

The tool uses publicly available provider data — accuracy depends on FCC BDC filings; small pockets may be misreported or missing.

The dashboard does not include private user data or sensitive information — only public availability and provider data.

For now, there is no user authentication or role-based permissions; the tool is intended for internal use by county/state broadband offices.

The data processing pipeline may take several minutes depending on system RAM when loading full-state data.
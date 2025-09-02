import requests
import pandas as pd
import sqlite3
import streamlit as st

API_KEY = "e1a131d6-f1fb-440c-8f9c-fbe475876497"
OBJECT_URL = "https://api.harvardartmuseums.org/object"

SOURCE_DB = "mydatabase.db"   # Temp storage for fetched classification data
DEST_DB = "totaldata.db"      # Final storage for inserted data


def fetch_classification_data(classification, pages=25, size=100):
    """Fetch records for a single classification from Harvard Art Museum API."""
    all_records = []

    for page in range(1, pages + 1):
        params = {
            "apikey": API_KEY,
            "size": size,
            "page": page,
            "classification": classification,
        }
        response = requests.get(OBJECT_URL, params=params)

        if response.status_code != 200:
            st.error(f"❌ Error fetching {classification} page {page}")
            break

        data = response.json()
        if "records" in data:
            all_records.extend(data["records"])
        else:
            break

    return all_records


def process_data(records):
    """Convert API records into structured tables (metadata, media, colors)."""
    artifact_metadata = []
    artifact_media = []
    artifact_colors = []

    for i in records:
        artifact_metadata.append(dict(
            id=i.get('id'),
            title=i.get('title'),
            culture=i.get('culture'),
            period=i.get('period'),
            century=i.get('century'),
            medium=i.get('medium'),
            dimensions=i.get('dimensions'),
            description=i.get('description'),
            department=i.get('department'),
            classification=i.get('classification'),
            accessionyear=i.get('accessionyear'),
            accessionmethod=i.get('accessionmethod'),
        ))

        artifact_media.append(dict(
            objectid=i.get('objectid'),
            imagecount=i.get('imagecount'),
            mediacount=i.get('mediacount'),
            colorcount=i.get('colorcount'),
            rank=i.get('rank'),
            datebegin=i.get('datebegin'),
            dateend=i.get('dateend'),
        ))

        color_details = i.get('colors')
        if color_details:
            for j in color_details:
                artifact_colors.append(dict(
                    objectid=i.get('objectid'),
                    color=j.get('color'),
                    spectrum=j.get('spectrum'),
                    hue=j.get('hue'),
                    percent=j.get('percent'),
                    css3=j.get('css3'),
                ))

    return (
        pd.DataFrame(artifact_metadata),
        pd.DataFrame(artifact_media),
        pd.DataFrame(artifact_colors),
    )


def save_to_source_db(df_metadata, df_media, df_colors):
    """Save fetched classification data into SOURCE_DB (temporary)."""
    conn = sqlite3.connect(SOURCE_DB)

    # Recreate schema each time
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS artifact_metadata")
    cursor.execute("""CREATE TABLE artifact_metadata (
        id INTEGER PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INTEGER,
        accessionmethod TEXT
    )""")

    cursor.execute("DROP TABLE IF EXISTS artifact_media")
    cursor.execute("""CREATE TABLE artifact_media (
        objectid INTEGER,
        imagecount INTEGER,
        mediacount INTEGER,
        colorcount INTEGER,
        rank INTEGER,
        datebegin INTEGER,
        dateend INTEGER,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    )""")

    cursor.execute("DROP TABLE IF EXISTS artifact_colors")
    cursor.execute("""CREATE TABLE artifact_colors (
        objectid INTEGER,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    )""")
    conn.commit()

    df_metadata.to_sql("artifact_metadata", conn, if_exists="append", index=False)
    df_media.to_sql("artifact_media", conn, if_exists="append", index=False)
    df_colors.to_sql("artifact_colors", conn, if_exists="append", index=False)

    conn.close()


# STREAMLIT APP

st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'>🏛️ Harvards Artifact Collections 🏛️</h1>", unsafe_allow_html=True)

# Dropdown
option = st.selectbox(
    "Please select the Classification",
    ("Archival Material", "Photographs", "Drawings", "Coins", "Prints", "Fragments")
)
st.write("You selected:", option)

# Buttons
col1, col2, col3 = st.columns(3)
with col1:
    collect_btn = st.button("📊 Collect Data", use_container_width=True)
with col2:
    insert_btn = st.button("✅ Insert Into SQL", use_container_width=True)
with col3:
    sql_btn = st.button("🔎 SQL Queries", use_container_width=True)

# Collect Data
if collect_btn:
    records = fetch_classification_data(option)
    if not records:
        st.warning("⚠️ No data fetched!")
    else:
        df_metadata, df_media, df_colors = process_data(records)
        save_to_source_db(df_metadata, df_media, df_colors)

        st.session_state["df_metadata"] = df_metadata
        st.session_state["df_media"] = df_media
        st.session_state["df_colors"] = df_colors

        st.success(f"✅ Collected {len(df_metadata)} artifacts for '{option}'")

# Insert Data
if insert_btn:
    if "df_metadata" not in st.session_state:
        st.error("⚠️ Please collect data first before inserting!")
    else:
        conn_dest = sqlite3.connect(DEST_DB)
        existing = pd.read_sql(
            "SELECT * FROM artifact_metadata WHERE classification = ?",
            conn_dest, params=(option,)
        )

        if not existing.empty:
            st.warning(f"⚠️ Data for '{option}' already exists in SQL!")
        else:
            st.session_state["df_metadata"].to_sql("artifact_metadata", conn_dest, if_exists="append", index=False)
            st.session_state["df_media"].to_sql("artifact_media", conn_dest, if_exists="append", index=False)
            st.session_state["df_colors"].to_sql("artifact_colors", conn_dest, if_exists="append", index=False)
            st.success(f"✅ Inserted classification '{option}' successfully into SQL")

        conn_dest.close()

# SQL Query
if sql_btn:
    st.session_state["show_query_dropdown"] = True  # store state

if st.session_state.get("show_query_dropdown", False):
    st.subheader("🔎 Run SQL Query")
    sql_queries = {
        "Show all metadata": "SELECT * FROM artifact_metadata ",
        "Show all media": "SELECT * FROM artifact_media ",
        "Show all colors": "SELECT * FROM artifact_colors LIMIT 20",
        "List all artifacts from the 11th century belonging to Byzantine culture.": "SELECT * FROM artifact_metadata WHERE culture = 'Byzantine' AND century = '11th century'",
        "Count artifacts by classification": "SELECT classification, COUNT(*) as total FROM artifact_metadata GROUP BY classification",
        "Top 10 recent artifacts": "SELECT * FROM artifact_metadata ORDER BY id DESC LIMIT 10",
        "What are the unique cultures represented in the artifacts?": "SELECT DISTINCT culture FROM artifact_metadata",
        "List all artifacts from the Archaic Period.": "SELECT * FROM artifact_metadata WHERE period='Archaic period'",
        "List artifact titles ordered by accession year in descending order.": "SELECT title,accessionyear FROM artifact_metadata ORDER BY accessionyear DESC",
        "How many artifacts are there per department?": "SELECT department, count(*) AS 'Artifacts per department' FROM artifact_metadata GROUP BY department",
        "Which artifacts have more than 1 image?": "SELECT * FROM artifact_media WHERE imagecount >1",
        "What is the average rank of all artifacts?": "SELECT avg(rank) FROM artifact_media",
        "Which artifacts have a higher colorcount than mediacount?": "SELECT * FROM artifact_media WHERE colorcount > mediacount",
        "List all artifacts created between 1500 and 1600.": "SELECT * FROM artifact_media WHERE datebegin >=1500 AND dateend<=1600;",
        "How many artifacts have no media files?": "SELECT COUNT(mediacount) AS count_of_artifacts_has_no_media_files FROM artifact_media WHERE mediacount=0 GROUP BY mediacount",
        "What are all the distinct hues used in the dataset?": "SELECT DISTINCT hue FROM artifact_colors",
        "What are the top 5 most used colors by frequency?": "SELECT color, COUNT(*) AS frequency FROM artifact_colors GROUP BY color ORDER BY frequency DESC LIMIT 5;",
        "What is the average coverage percentage for each hue?": "SELECT hue,AVG(percent) AS Average_Percentage FROM artifact_colors GROUP BY hue",
        "List all colors used for a given 280069.":"SELECT objectid, color FROM artifact_colors WHERE objectid = 280069",
        "What is the total number of color entries in the dataset.": "SELECT COUNT(color) AS Total_no_of_colors FROM artifact_colors",
        "List artifact titles and hues for all artifacts belonging to the Byzantine culture.": "SELECT m.title,m.culture, c.hue FROM artifact_metadata m INNER JOIN artifact_colors c ON m.id = c.objectid WHERE m.culture = 'Byzantine';",
        "List each artifact title with its associated hues.": "SELECT m.title,c.hue FROM artifact_metadata m INNER JOIN artifact_colors c ON m.id = c.objectid",
        "Get artifact titles, cultures, and media ranks where the period is not null.": "SELECT m.title, m.culture, md.rank FROM artifact_metadata m INNER JOIN artifact_media md ON m.id = md.objectid WHERE m.period IS NOT NULL",
        "Find artifact titles ranked in the top 10 that include the color hue Grey.": "SELECT m.title, MIN(md.rank) AS top_rank, c.hue FROM artifact_metadata m INNER JOIN artifact_media md ON m.id = md.objectid INNER JOIN artifact_colors c ON m.id = c.objectid WHERE c.hue = 'Grey' GROUP BY m.title, c.hue ORDER BY top_rank ASC LIMIT 10;",
        "How many artifacts exist per classification, and what is the average media count for each?": "SELECT m.classification, COUNT(DISTINCT m.id) AS total_artifacts, AVG(media_count) AS avg_media_per_artifact FROM artifact_metadata m LEFT JOIN (SELECT objectid, COUNT(*) AS media_count FROM artifact_media GROUP BY objectid ) md ON m.id = md.objectid GROUP BY m.classification ORDER BY total_artifacts DESC;",
        "Top 5 cultures with the most artifacts.": "SELECT culture, COUNT(*) AS total_artifacts FROM artifact_metadata WHERE culture IS NOT NULL GROUP BY culture ORDER BY total_artifacts DESC LIMIT 5;",
        "Top 5 most used colors across all artifacts.": "SELECT c.hue, COUNT(*) AS usage_count FROM artifact_colors c GROUP BY c.hue ORDER BY usage_count DESC LIMIT 5;",
        "Count artifacts per classification with average media count.":"SELECT m.classification, COUNT(DISTINCT m.id) AS total_artifacts, AVG(media_count) AS avg_media_per_artifact FROM artifact_metadata m LEFT JOIN (SELECT objectid, COUNT(*) AS media_count FROM artifact_media GROUP BY objectid ) md ON m.id = md.objectid GROUP BY m.classification ORDER BY total_artifacts DESC;",
        "Find all artifacts with unknown culture.":"SELECT id, title FROM artifact_metadata WHERE culture IS NULL OR culture = '' GROUP BY title"
    }

    selected_query = st.selectbox("Choose a query", list(sql_queries.keys()), key="query_select")

    if st.button("Run Query", use_container_width=True, key="run_query"):
        try:
            conn = sqlite3.connect(DEST_DB)
            df_query = pd.read_sql(sql_queries[selected_query], conn)
            conn.close()
            st.subheader(f"📊 Result for: {selected_query}")
            st.dataframe(df_query)
        except Exception as e:
            st.error(f"❌ Error: {e}")

# Display collected data
if "df_metadata" in st.session_state:
    st.subheader("📌 Artifact Metadata")
    st.dataframe(st.session_state["df_metadata"])
if "df_media" in st.session_state:
    st.subheader("📌 Artifact Media")
    st.dataframe(st.session_state["df_media"])
if "df_colors" in st.session_state:
    st.subheader("📌 Artifact Colors")
    st.dataframe(st.session_state["df_colors"])


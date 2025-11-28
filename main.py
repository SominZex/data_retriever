# main.py
import streamlit as st
import runpy
from pathlib import Path

st.set_page_config(page_title="Unified Dashboard (Choose App)", page_icon="🧩", layout="wide")
st.title("🧩 Unified Sales Dashboard")

APP_MAP = {
    "CSV Downloader": "down.py",
    "Store Analytics Dashboard": "store.py",
}

choice = st.selectbox("Choose an app to run", list(APP_MAP.keys()))

# show a small note so it's clear only one app executes
st.caption("Only the selected app file will be executed. To switch apps, choose from the dropdown above.")

# execute only the selected file (in its own new namespace)
selected_file = Path(APP_MAP[choice])

if not selected_file.exists():
    st.error(f"File not found: {selected_file}")
else:
    try:
        # run_path executes the file in a fresh namespace (so global names inside down.py/store.py won't leak here)
        runpy.run_path(str(selected_file), run_name="__main__")
    except Exception as e:
        st.error("An error occurred while running the selected app.")
        st.exception(e)

# main.py
import streamlit as st
import runpy
from pathlib import Path

st.set_page_config(page_title="Unified Dashboard (Choose App)", page_icon="🧩", layout="wide")

# -----------------------------
# LOGIN SYSTEM (Secure)
# -----------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def login_screen():
    st.title("🔐 Secure Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login", type="primary"):
        if (
            username == st.secrets["LOGIN_USER"]
            and password == st.secrets["LOGIN_PASS"]
        ):
            st.session_state.logged_in = True
            st.success("Login successful! Redirecting...")
            st.rerun()
        else:
            st.error("Invalid username or password")

# If not logged in → show login screen only
if not st.session_state.logged_in:
    login_screen()
    st.stop()

# -----------------------------
# MAIN APP (after login)
# -----------------------------
st.title("🧩 Unified Sales Dashboard")

# Logout button
if st.sidebar.button("🚪 Logout"):
    st.session_state.logged_in = False
    st.rerun()

APP_MAP = {
    "CSV Downloader": "down.py",
    "Store Analytics Dashboard": "store.py",
}

choice = st.selectbox("Choose an app to run", list(APP_MAP.keys()))

st.caption("Only the selected app will be executed. To switch apps, choose from the dropdown above.")

selected_file = Path(APP_MAP[choice])

if not selected_file.exists():
    st.error(f"File not found: {selected_file}")
else:
    try:
        runpy.run_path(str(selected_file), run_name="__main__")
    except Exception as e:
        st.error("An error occurred while running the selected app.")
        st.exception(e)

# main.py
import streamlit as st
import runpy
from pathlib import Path

st.set_page_config(page_title="Unified Dashboard (Choose App)", page_icon="🧩", layout="wide")

# -----------------------------
# LOGIN SYSTEM (Secure)
# -----------------------------
# -----------------------------
# MULTI-USER LOGIN SYSTEM
# -----------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "username" not in st.session_state:
    st.session_state.username = None

def login_screen():
    st.title("🔐 Secure Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login", type="primary"):

        users = st.secrets["USERS"]  # dictionary of users

        if username in users and password == users[username]:
            st.session_state.logged_in = True
            st.session_state.username = username
            st.success(f"Welcome, {username}! Redirecting...")
            st.rerun()
        else:
            st.error("Invalid username or password")

# If not logged in → show login screen
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

# main.py
import streamlit as st
import runpy
from pathlib import Path

st.set_page_config(page_title="New Shop Unified Dashboard", page_icon="🧩", layout="wide")

# ---------------------------------------
# LOGIN & ROLE SYSTEM
# ---------------------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "username" not in st.session_state:
    st.session_state.username = None

def login_screen():
    st.title("🔐 Login to Dashboard")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login", type="primary"):
        users = st.secrets["USERS"]

        if username in users and password == users[username]:
            st.session_state.logged_in = True
            st.session_state.username = username
            st.success(f"Welcome, {username}! Redirecting...")
            st.rerun()
        else:
            st.error("Invalid username or password")

# Show login screen if not logged in
if not st.session_state.logged_in:
    login_screen()
    st.stop()

# ---------------------------------------
# ROLE-BASED ACCESS CONTROL
# ---------------------------------------
username = st.session_state.username
role = st.secrets["ROLES"].get(username, "none")

# Determine allowed apps based on role
if role == "all":  
    allowed_apps = {
        "CSV Downloader": "down.py",
        "Store Analytics Dashboard": "store.py",
    }
elif role == "store_only":  
    allowed_apps = {
        "Store Analytics Dashboard": "store.py",
    }
elif role == "csv_only":  
    allowed_apps = {
        "CSV Downloader": "down.py",
    }
else:
    allowed_apps = {}

# Logout button
if st.sidebar.button("🚪 Logout"):
    st.session_state.logged_in = False
    st.rerun()

# ---------------------------------------
# MAIN APP INTERFACE (after login)
# ---------------------------------------
st.title("🧩 Unified Sales Dashboard")

if not allowed_apps:
    st.error("You do not have permission to access any app.")
    st.stop()

choice = st.selectbox("Choose an app to run", list(allowed_apps.keys()))

st.caption("Only the selected app will run. To switch apps, choose from the dropdown above.")

selected_file = Path(allowed_apps[choice])

# ---------------------------------------
# EXECUTE ONLY THE SELECTED APP
# ---------------------------------------
if not selected_file.exists():
    st.error(f"File not found: {selected_file}")
else:
    try:
        runpy.run_path(str(selected_file), run_name="__main__")
    except Exception as e:
        st.error("An error occurred while running the selected app.")
        st.exception(e)

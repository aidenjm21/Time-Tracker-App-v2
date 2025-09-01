import psycopg2
import streamlit as st
import socket, os

# --- Database connection ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=int(st.secrets["postgres"]["port"]),
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"]
    )

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM my_table LIMIT 5;")
rows = cur.fetchall()
st.write("Sample rows:", rows)

# --- Optional diagnostics ---
with st.expander("Diagnostics"):
    target = ("100.121.48.50", 5433)
    st.write("Hostname:", socket.gethostname())
    st.write("Env hint:", os.environ.get("HOSTNAME") or os.environ.get("COMPUTERNAME"))

    # List local IPs
    try:
        host, aliases, addrs = socket.gethostbyname_ex(socket.gethostname())
        st.write("Local IPs:", addrs)
    except Exception as e:
        st.write("Local IPs lookup failed:", repr(e))

    # TCP probe to Postgres
    try:
        sock = socket.create_connection(target, timeout=5)
        st.success(f"TCP OK to {target}, from local {sock.getsockname()}")
        sock.close()
    except Exception as e:
        st.error(f"TCP failed to {target}: {e!r}")

    st.write("DB URL:", st.secrets["database"]["url"])

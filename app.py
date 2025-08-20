import socket, os, streamlit as st

target = ("192.168.1.128", 5433)
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

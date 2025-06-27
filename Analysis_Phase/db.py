import os
import oracledb
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load config from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "service_name": os.getenv("DB_SERVICE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Build DSN (Oracle thin connection string)
dsn = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['service_name']}"

# Establish connection
try:
    connection = oracledb.connect(
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dsn=dsn
    )
    cursor = connection.cursor()
except Exception as e:
    raise RuntimeError(f"Failed to connect to Oracle DB: {e}")

# SQL execution function
def run_sql(query: str, retries: int = 2):
    for attempt in range(retries):
        try:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            if attempt < retries - 1:
                continue
            return f"[SQL Error] {str(e)}"

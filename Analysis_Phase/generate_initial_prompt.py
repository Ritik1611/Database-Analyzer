import os
import oracledb
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "service_name": os.getenv("DB_SERVICE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
dsn = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['service_name']}"

# Connect to Oracle
connection = oracledb.connect(
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    dsn=dsn,
    encoding="UTF-8"
)
cursor = connection.cursor()

# Get list of tables owned by PROP
cursor.execute("""
    SELECT table_name
    FROM all_tables
    WHERE owner = 'PROP'
    ORDER BY table_name
""")
tables = [row[0] for row in cursor.fetchall()]

lines = []

for table in tables:
    lines.append(f"Table: {table}")

    cursor.execute("""
        SELECT column_name, data_type
        FROM all_tab_columns
        WHERE owner = 'PROP' AND table_name = :table
        ORDER BY column_id
    """, [table])

    columns = cursor.fetchall()
    col_line = "Columns: " + ", ".join(f"{col} ({dtype})" for col, dtype in columns)
    lines.append(col_line)
    lines.append("")  # blank line between tables

# Write to file
with open("initial_prompt.txt", "w") as f:
    f.write("\n".join(lines))

print("✅ initial_prompt.txt generated.")

# Cleanup
cursor.close()
connection.close()
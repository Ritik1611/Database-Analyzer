import sqlite3
from datetime import datetime

DB_PATH = "agent_logs.db"

def ensure_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            step INTEGER,
            query TEXT,
            result TEXT,
            explanation TEXT,
            error TEXT
        )
    """)
    conn.commit()

# Called during every analysis step
def log(step, query, result, explanation, error):
    conn = sqlite3.connect(DB_PATH)
    ensure_table_exists(conn)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs (timestamp, step, query, result, explanation, error)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), step, query, result, explanation, error))
    conn.commit()
    conn.close()

# Fetch logs related to a specific table (RAG source)
def fetch_logs_for_table(table_name, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    ensure_table_exists(conn)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT step, query, explanation, error FROM logs
        WHERE LOWER(query) LIKE ?
        ORDER BY step ASC
    """, (f"%{table_name.lower()}%",))
    rows = cursor.fetchall()
    conn.close()
    return rows

# Convert logs into prompt-compatible format (limited to recent entries)
def format_logs_as_context(logs, max_entries=5):
    context = []
    for step, query, explanation, error in logs:
        if query:
            context.append(f"[Step {step}] SQL: {query}")
        if explanation:
            context.append(f"[Step {step}] Explanation: {explanation}")
        if error and error.strip() and error.lower() != "none":
            context.append(f"[Step {step}] Error: {error}")
    return "\n".join(context[-max_entries:])

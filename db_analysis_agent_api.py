import os
import re
import oracledb
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# Load env
load_dotenv()

# --- Mistral API Client ---
client = OpenAI(
    api_key=os.getenv("MISTRAL_API_KEY"),
    base_url="https://api.mistral.ai/v1"
)
model_id = "mistral-small-latest"

# --- Oracle DB Setup ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "service_name": os.getenv("DB_SERVICE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
dsn = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['service_name']}"
connection = oracledb.connect(user=DB_CONFIG['user'], password=DB_CONFIG['password'], dsn=dsn)
cursor = connection.cursor()

# --- SQL Executor ---
def run_sql(query: str):
    try:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        return df.head(5).to_markdown(index=False)
    except Exception as e:
        return f"Error: {str(e)}"

# --- Load schema from file and split into parts ---
def split_schema(file_path: str, max_lines_per_chunk: int = 20):
    with open(file_path, "r") as f:
        lines = f.readlines()

    chunks = []
    current = []
    for line in lines:
        if line.startswith("Table:") and current:
            chunks.append("".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        chunks.append("".join(current))
    return chunks

schema_chunks = split_schema("initial_prompt.txt", max_lines_per_chunk=20)

# --- Initial System Instruction ---
history = [
    "role": "system",
        "content": (
            "You are a data analyst agent connected to an Oracle SQL database.\n"
            "You are not allowed to manipulate the database directly. Never use INSERT, UPDATE, DELETE, or DROP statements.\n"
            "1. Detect duplicate rows or dummy data in each table.\n"
            "2. Identify relationships between tables (primary/foreign keys).\n"
            "3. Describe the distribution and structure of data in each table.\n"
            "4. Explain what each table most likely stores.\n"
            "5. Suggest any insights or anomalies.\n"
            "6. Provide SQL queries to extract relevant data.\n"
            "7. Provide explanation after analyzing each table and its columns.\n"
            "8. If you encounter any errors, provide the SQL error message.\n"
            "9. Always include the database name before the table name in the SQL queries. (e.g., PROP.table_name)\n"
            "10. Run join queries too, to get more insights on the relationships between tables.\n\n"
            "Your responses must follow this format:\n"
            "SQL: <your SQL query>\nExplanation: <your explanation>\nError: <your error>"
        )
    ]

executed_queries = set()

# --- Process each table chunk ---
for step, table_schema in enumerate(schema_chunks):
    print(f"\n== Processing Table {step+1} ==\n")
    history.append({"role": "user", "content": f"Schema:\n{table_schema.strip()}"})

    # Get response
    response = client.chat.completions.create(
        model=model_id,
        messages=history,
        temperature=0.7
    )
    msg = response.choices[0].message.content
    print(msg)
    history.append({"role": "assistant", "content": msg})

    # Parse response
    sql_match = re.search(r"SQL:\s*(SELECT .*?)(?:\n|$)", msg, re.DOTALL | re.IGNORECASE)
    explanation_match = re.search(r"Explanation:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)
    error_match = re.search(r"Error:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)

    if sql_match:
        query = sql_match.group(1).strip().rstrip(";")
        if query in executed_queries:
            print("[Repeated query, skipping]")
            continue

        print(f"\nRunning SQL:\n{query}")
        output = run_sql(query)
        print("Result:\n", output)
        history.append({"role": "user", "content": f"Result of `{query}`:\n{output}"})
        executed_queries.add(query)

    elif explanation_match:
        print("Explanation:\n", explanation_match.group(1).strip())
        history.append({"role": "user", "content": "Thanks. Next table?"})

    elif error_match:
        print("SQL Error:\n", error_match.group(1).strip())
        break

    else:
        print("[No structured output found]")
        break

import os
import re
import oracledb
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from io import StringIO

# Load environment variables
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
        return df
    except Exception as e:
        return f"Error: {str(e)}"

# --- Save query result to CSV ---
def save_to_csv(df: pd.DataFrame, step: int):
    filename = f"query_result_step{step}.csv"
    df.to_csv(filename, index=False)
    print(f"[Saved to {filename}]")

# --- Load schema from file and split ---
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

# --- System Instructions ---
history = [
    {   "role": "system",
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
            "9. Always include the database name (here, for all the tables the database name is PROP) before the table name in the SQL queries. (e.g., PROP.table_name)\n"
            "10. Run join queries too, to get more insights on the relationships between tables.\n"
            "11. Provide a summary of findings after analyzing each table.\n"
            "12. If you cannot analyze a table, provide a reason why.\n"
            "13. If you need more information about the table, get it using any SQL query.\n"
            "14. Usually, you'll be provided with the result of the SQL query you just gave. You can use that result to analyze the table further.\n"
            "15. The SQL queries you provide will be executed against the Oracle database.\n"
            "16. Provide one SQL query at a time, and wait for the result before proceeding.\n"
            "17. If you have finished analyzing the table, ask for the next table. by saying 'Next table?'\n"
            "Your responses must follow this format:\n"
            "SQL: <your SQL query>\nExplanation: <your explanation>\nError: <your error>"
        )
    }
]

executed_queries = set()

# --- Main Loop ---
for step, table_schema in enumerate(schema_chunks):
    print(f"\n== Processing Table {step+1} ==\n")
    history.append({"role": "user", "content": f"Schema:\n{table_schema.strip()}"})

    # Generate response
    response = client.chat.completions.create(
        model=model_id,
        messages=history,
        temperature=0.7
    )
    msg = response.choices[0].message.content
    print(msg)
    history.append({"role": "assistant", "content": msg})

    # Match patterns
    sql_match = re.search(r"SQL:\s*(SELECT .*?)(?:\n|$)", msg, re.DOTALL | re.IGNORECASE)
    explanation_match = re.search(r"Explanation:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)
    error_match = re.search(r"Error:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)

    if sql_match:
        query = sql_match.group(1).strip().rstrip(";")
        if query in executed_queries:
            print("[Repeated query, skipping]")
            continue

        print(f"\nRunning SQL:\n{query}")
        result = run_sql(query)

        if isinstance(result, str) and result.startswith("Error"):
            print(result)
            history.append({"role": "user", "content": f"Result of `{query}`:\n{result}"})
            continue

        # Print + Save
        print("Query Result:\n", result.head(5).to_markdown(index=False))
        save_to_csv(result, step)
        history.append({"role": "user", "content": f"Result of `{query}`:\n{result.head(5).to_markdown(index=False)}"})
        executed_queries.add(query)

    elif explanation_match:
        explanation = explanation_match.group(1).strip()
        print("Explanation:\n", explanation)
        history.append({"role": "user", "content": "Thanks. Next table?"})

    elif error_match:
        print("SQL Error:\n", error_match.group(1).strip())
        break

    else:
        print("[No structured output found]")
        break

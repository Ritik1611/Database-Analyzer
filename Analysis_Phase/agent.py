import os
import re
import time
from openai import OpenAI
from db import run_sql
from logger import log, fetch_logs_for_table, format_logs_as_context
import pandas as pd

client = OpenAI(api_key=os.getenv("MISTRAL_API_KEY"), base_url="https://api.mistral.ai/v1")
model_id = "mistral-small-latest"

SYSTEM_PROMPT = [
    {
        "role": "system",
        "content": (
            "You are an intelligent data analyst agent connected to an Oracle SQL database. "
            "You analyze one table at a time using read-only SQL queries. "
            "You **must never** modify the database. That means: never use INSERT, UPDATE, DELETE, DROP, or TRUNCATE statements.\n\n"

            "### Your Responsibilities (Follow in Order):\n"
            "1. Start by explaining what the current table likely stores based on its schema.\n"
            "2. Analyze the table by running one SQL query at a time to:\n"
            "   - Detect duplicate or dummy data (e.g., fields with values like 'test', empty fields, repeated rows).\n"
            "   - Describe the structure and distribution of the data (e.g., distinct values, common nulls, ranges).\n"
            "   - Identify relationships with other tables (foreign keys, joinable fields).\n"
            "   - Highlight any insights, data quality issues, or anomalies you discover.\n"
            "3. Use JOIN queries where necessary to explore relationships.\n"
            "4. Provide SQL queries to extract the data needed for analysis. These must:\n"
            "   - Include the full database name prefix: `PROP.` (e.g., `SELECT * FROM PROP.table_name ...`)\n"
            "   - Be single-line queries only (no multiline SQL).\n"
            "   - Retrieve only relevant data (no SELECT * unless needed).\n"
            "   - Use `ROWNUM <= 5` to limit large outputs.\n"
            "5. After each query is executed, use the result (which will be provided to you) to explain what it reveals.\n"
            "6. Once you’ve completed all useful insights for the current table, say:\n"
            "   **Next table?** — and include a full summary of everything you’ve found so far:\n"
            "   - What the table stores\n"
            "   - Any anomalies, dummy data, or duplicates\n"
            "   - Structure or value distribution\n"
            "   - Relationships with other tables (using joins, foreign keys, etc.)\n\n"

            "### Output Format (Strictly Follow This):\n"
            "Explanation: <your explanation based on the last query result>\n"
            "SQL: <your next SQL query to run>\n"
            "Error: <if any error occurred, write it here; otherwise write 'None'>\n\n"

            "### Additional Rules:\n"
            "- Wait for the result of your query before continuing.\n"
            "- Only one SQL query per response.\n"
            "- Do not repeat queries already executed.\n"
            "- Avoid redundant summaries before asking for the next table.\n"
            "- If no analysis is possible, explain why (e.g., insufficient data).\n"
            "- You should NEVER hallucinate data or results. Only describe what’s visible or can be queried.\n"
        )
    }
]


def extract_table_name(schema: str) -> str:
    match = re.search(r"CREATE\s+TABLE\s+(\w+)", schema, re.IGNORECASE)
    return match.group(1) if match else "unknown_table"

def clean_cell(val):
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8", errors="replace")
        except:
            return "<binary>"
    elif isinstance(val, (list, tuple)):
        return str([clean_cell(v) for v in val])
    elif val is None:
        return ""
    return str(val)

def process_schema_chunks(chunks, start_index=0):
    for step, schema in enumerate(chunks[start_index:], start=start_index):
        table_name = extract_table_name(schema)
        previous_logs = fetch_logs_for_table(table_name)
        prior_context = format_logs_as_context(previous_logs)

        executed_queries = {row[1] for row in previous_logs if row[1]}
        rounds = 0
        table_done = False

        while not table_done and rounds < 5:
            rounds += 1

            messages = SYSTEM_PROMPT + [{
                "role": "user",
                "content": f"Schema:\n{schema.strip()}\n\nPrevious analysis:\n{prior_context}"
            }]

            time.sleep(1.1)  # Mistral rate limit

            response = client.chat.completions.create(
                model=model_id,
                messages=messages,
                temperature=0.7
            )
            msg = response.choices[0].message.content
            print(f"\nStep {step+1} - Round {rounds}\n{msg}")

            sql_match = re.search(r"SQL:\s*```sql\s*(.*?)\s*```", msg, re.DOTALL | re.IGNORECASE)
            if not sql_match:
                sql_match = re.search(r"SQL:\s*(SELECT .*?)(?:\n|$)", msg, re.DOTALL | re.IGNORECASE)

            explanation = re.search(r"Explanation:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)
            error = re.search(r"Error:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)

            sql_text = sql_match.group(1).strip().rstrip(";") if sql_match else None
            explanation_text = explanation.group(1).strip() if explanation else ""
            error_text = error.group(1).strip() if error else ""

            print(explanation_text)

            if sql_text:
                if sql_text in executed_queries:
                    print("[Duplicate query. Skipping.]")
                    break

                result = run_sql(sql_text)
                if isinstance(result, str) and result.startswith("Error"):
                    error_text = result
                    result_preview = error_text
                else:
                    if isinstance(result, str):
                        result_preview = result
                    else:
                        safe_result = result.copy()
                        for col in safe_result.columns:
                            safe_result[col] = safe_result[col].apply(clean_cell)

                        result_preview = safe_result.head(5).to_markdown(index=False)

                print(result_preview)
                executed_queries.add(sql_text)
                log(step, sql_text, str(result), explanation_text, error_text)
            else:
                log(step, None, None, explanation_text, error_text)

            if "Next table?" in msg:
                table_done = True

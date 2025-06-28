import os
import re
from openai import OpenAI
from db import run_sql
from logger import log, fetch_logs_for_table, format_logs_as_context
import time

client = OpenAI(api_key=os.getenv("MISTRAL_API_KEY"), base_url="https://api.mistral.ai/v1")
model_id = "mistral-small-latest"

SYSTEM_PROMPT = [
    {
        "role": "system",
        "content": (
            "You are a data analyst agent connected to an Oracle SQL database.\n"
            "You are not allowed to manipulate the database directly. Never use INSERT, UPDATE, DELETE, or DROP statements.\n"
            "1. Detect duplicate rows or dummy data (like the records containing 'test'(case insensitive) or values that should not be on the table) in the table.\n"
            "2. Identify relationships with other tables (primary/foreign keys).\n"
            "3. Describe the distribution and structure of data in the table.\n"
            "4. Explain what the table most likely stores.\n"
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
            "16. Provide one SQL query at a time, and wait for the result before proceeding. The queries should be given in a single line.\n"
            "17. If you have finished analyzing the table, ask for the next table. by saying 'Next table?' After this, you should provide the complete detailed summary of the table regarding the relationships of it with other tables, including any foreign key relationships and how they connect to other tables. Also include the summary of everything done in till now in the table. Include this in the Explanation section of the response.\n"
            "Your responses must follow this format:\n"
            "Explanation: <your explanation_of_the_previous_sql_query_result>\nSQL: <your SQL query>\nError: <your error>"
        )
    }
]

def extract_table_name(schema: str) -> str:
    # Tries to extract table name from CREATE TABLE lines
    match = re.search(r"CREATE\s+TABLE\s+(\w+)", schema, re.IGNORECASE)
    return match.group(1) if match else "unknown_table"

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
            current_step = step

            messages = SYSTEM_PROMPT + [{
                "role": "user",
                "content": f"Schema:\n{schema.strip()}\n\nPrevious analysis:\n{prior_context}"
            }]

            time.sleep(1.1)

            response = client.chat.completions.create(
                model=model_id,
                messages=messages,
                temperature=0.7
            )
            msg = response.choices[0].message.content
            print(f"\nStep {step+1} - Round {rounds}\n{msg}")

            sql = re.search(r"SQL:\s*```sql\s*(.*?)\s*```", msg, re.DOTALL | re.IGNORECASE)
            if not sql:
                sql = re.search(r"SQL:\s*(SELECT .*?)(?:\n|$)", msg, re.DOTALL | re.IGNORECASE)
            explanation = re.search(r"Explanation:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)
            error = re.search(r"Error:\s*(.*)", msg, re.DOTALL | re.IGNORECASE)

            sql_text = sql.group(1).strip().rstrip(";") if sql else None
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
                        # Convert all bytes columns to string for preview
                        safe_result = result.copy()
                        for col in safe_result.columns:
                            if safe_result[col].dtype == object:
                                safe_result[col] = safe_result[col].apply(
                                    lambda x: x.decode('utf-8', errors='replace') if isinstance(x, bytes) else x
                                )

                        result_preview = safe_result.head(5).to_markdown(index=False)

                print(result_preview)
                executed_queries.add(sql_text)
                log(current_step, sql_text, str(result), explanation_text, error_text)
            else:
                log(current_step, None, None, explanation_text, error_text)

            if "Next table?" in msg:
                table_done = True

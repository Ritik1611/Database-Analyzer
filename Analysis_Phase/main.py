from dotenv import load_dotenv
from schema_loader import load_schema_chunks
from agent import process_schema_chunks

load_dotenv()

if __name__ == "__main__":
    schema_chunks = load_schema_chunks("initial_prompt.txt", max_lines=20)
    # Resume from a specific table number (0-based)
    start_index = int(input("Enter the table number to start from (0 for first table): ") or 0)
    process_schema_chunks(schema_chunks, start_index=start_index)

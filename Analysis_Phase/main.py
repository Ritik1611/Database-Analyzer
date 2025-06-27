from dotenv import load_dotenv
from schema_loader import load_schema_chunks
from agent import process_schema_chunks

load_dotenv()

if __name__ == "__main__":
    schema_chunks = load_schema_chunks("initial_prompt.txt", max_lines=20)
    process_schema_chunks(schema_chunks)

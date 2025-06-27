def load_schema_chunks(file_path: str, max_lines: int = 20):
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

import sqlite3
import json
import os

DB_PATH = "db/insightpilot.db"
SCHEMA_JSON_PATH = "db/schema_metadata.json"

os.makedirs("db", exist_ok=True)

def get_schema():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    tables = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()

    schema = {}
    for (table_name,) in tables:
        cols = cur.execute(f"PRAGMA table_info({table_name});").fetchall()
        schema[table_name] = [c[1] for c in cols]
    con.close()
    return schema

def main():
    schema = get_schema()
    print(json.dumps(schema, indent=2))

    with open(SCHEMA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    print(f"✅ Schema metadata saved to {SCHEMA_JSON_PATH}")

if __name__ == "__main__":
    main()

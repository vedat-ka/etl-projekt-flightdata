import sqlite3
from pathlib import Path


def split_sql_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    db_path = base_dir / "data" / "flights.sqlite"
    sql_path = base_dir / "sql" / "control_queries.sql"

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)

    connection = sqlite3.connect(db_path)
    try:
        for index, statement in enumerate(statements, start=1):
            print(f"\n--- Query {index} ---")
            print(statement)
            cursor = connection.execute(statement)
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
            else:
                print("(no rows)")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
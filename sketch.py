import sqlite3
from functools import cache


@cache
def open_db(file_name):
    return sqlite3.connect(file_name)


def iter_table(file_name, table_name):
    db = open_db(file_name)
    cur = db.cursor()
    cur.execute(f"SELECT * FROM {table_name};")
    while True:
        batch = cur.fetchmany()
        if not len(batch):
            break
        yield from batch


if __name__ == "__main__":
    for row in iter_table("data/sqlite.db", "entity_statement"):
        print(row)

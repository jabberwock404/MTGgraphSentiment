# Usage python add_column.py [Subreddit] [Field] [Type]
# Authors: JackLee9355
# Created: 2/18/2021

# The scope of this file is solely concerned with extracting fields from the json column and creating new columns for them in the database.
# This entire file is incredibly inefficient. But works due to infrequant use, it works fast enough.

import sqlite3
import sys
from config import DATABASES_PATH
import time
import json
import ast

def add_columns_before(database, field, before):
    cursor = database.cursor()
    cursor.execute("SELECT * FROM submissions WHERE created_utc < " + str(before) + " AND " + field + " IS NULL;")
    rows = cursor.fetchall()
    for row in rows:
        row_json = json.loads(row[2])
        # Use the row below if the database was dumped using str(dict) rather than json.dumps()
        # row_json = ast.literal_eval(row[2])
        cursor.execute("UPDATE submissions SET " + field + " = (?) WHERE id IS '" + str(row[0]) + "';", (row_json[field],))
    database.commit()

if __name__ == "__main__":    
    if len(sys.argv) != 4:
        print("Wrong number of arguments. Quiting")
        quit()
    subreddit = sys.argv[1]
    field = sys.argv[2]
    field_type = sys.argv[3]

    database = sqlite3.connect(DATABASES_PATH + subreddit + ".db")
    cursor = database.cursor()
    cursor.execute("SELECT * FROM pragma_table_info('submissions') WHERE name='" + field + "';")
    if len(cursor.fetchall()) == 0:
        cursor.execute("ALTER TABLE submissions ADD COLUMN " + field + " " + field_type + ";")

    min_utc = cursor.execute("SELECT MIN(created_utc) FROM submissions WHERE " + field + " IS NOT NULL;").fetchone()
    before = min_utc[0] if min_utc[0] is not None else int(time.time())

    add_columns_before(database, field, before)

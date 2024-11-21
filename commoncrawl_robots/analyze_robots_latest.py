import os
import datetime as dt
import argparse
import functools
import sqlite3
from multiprocessing import Pool
from collections import Counter

if __name__ == "__main__":
    version = 4
    out_dir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle/out"
    db_fullfilename = os.path.join(out_dir, f"everything_v{version}.db")

    print(dt.datetime.now(dt.UTC))
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        response = cursor.execute("SELECT * FROM latest_robots LIMIT 3;").fetchall()
        print(response)
        
        num_rows = cursor.execute("select max(RowId) from latest_robots;").fetchall()[0][0]
        print(num_rows)

        num_rows = cursor.execute("SELECT COUNT(*) FROM latest_robots;").fetchall()
        print(num_rows)
    
        cursor.close()

    print(dt.datetime.now(dt.UTC))
    print("done") 


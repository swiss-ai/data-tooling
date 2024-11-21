
import os
import argparse
import functools
import sqlite3
from multiprocessing import Pool
from collections import Counter


def get_num_rows(db_fullfilename: str) -> int:
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        # print(f"Opened SQLite database with version {sqlite3.sqlite_version}.")
        cursor = conn.cursor()
        # num_rows = cursor.execute("SELECT COUNT(1) FROM robots;").fetchall()[0][0]
        num_rows = cursor.execute("select max(RowId) from robotS;").fetchall()[0][0]
        # num_rows = cursor.execute("SELECT COUNT(*) FROM robots;").fetchall()[0][0]
    return num_rows


def get_first_rows(db_fullfilename: str) -> int:
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        print(conn.tables)
        cursor = conn.cursor()
        response = cursor.execute("SELECT * FROM robots LIMIT 3;").fetchall()
    return response


if __name__ == "__main__":
    version = 4
    base_dir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle/"
    db_dir = os.path.join(base_dir, "databases")
    out_dir = os.path.join(base_dir, "out")

    if False:
        db_files = list(filter(lambda _: _.endswith(f"_v{version}.db"), os.listdir(db_dir)))
        print(db_files)
        for db_file in db_files:
            db_fullfilename = os.path.join(db_dir, db_file)
            print(db_file, get_num_rows(db_fullfilename), get_first_rows(db_fullfilename))


    out_filename = f"everything_v{version}.db"
    out_fullfilename = os.path.join(base_dir, "out", out_filename)
    print(out_fullfilename)

    # query = "SELECT name FROM sqlite_master WHERE type='table';"
    query = "SELECT * from robots limit 3;"
    with sqlite3.connect(out_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        print(cursor.fetchall())


    print(f"Total rows = {get_num_rows(out_fullfilename)}")
   

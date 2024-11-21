import os
import datetime as dt
import argparse
import functools
import sqlite3
from multiprocessing import Pool
from collections import Counter

# https://travishorn.com/sql-group-with-most-recent-record-each

def do_grouping(db_fullfilename: str):
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        # response = cursor.execute("SELECT * FROM robots;").fetchall()

        inner_query = """
        SELECT    url,
                  MAX(timestamp) latest_timestamp
        FROM      robots
        GROUP BY  url
        """

        group_query = f"""
        SELECT robots.* 
        FROM robots
        JOIN ({inner_query}) iq
        ON robots.url = iq.url 
        AND robots.timestamp = iq.latest_timestamp
        """

        final_query = f"""
        CREATE TABLE latest_robots AS
        {group_query} 
        """
        response = cursor.execute(final_query)
        cursor.close()


if __name__ == "__main__":
    version = 4
    out_dir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle/out"
    fullfilename = os.path.join(out_dir, f"everything_v{version}.db")

    print(f"Before {dt.datetime.now(dt.UTC)}")
    print(f"db size before = {os.stat(fullfilename).st_size}")
    do_grouping(fullfilename)
    print(f"db size after = {os.stat(fullfilename).st_size}")
    print(f"After {dt.datetime.now(dt.UTC)}")
    print("Done")


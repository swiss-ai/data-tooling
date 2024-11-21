import os
import datetime as dt
import argparse
import functools
import sqlite3
from multiprocessing import Pool
from collections import Counter

# https://travishorn.com/sql-group-with-most-recent-record-each


def initialize_db(db_fullfilename: str):
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        command = """
         CREATE TABLE IF NOT EXISTS robots (
         url TEXT, 
         timestamp FLOAT,
         response INT, 
         content TEXT,
         provenance TEXT
         );
        """
        cursor.execute(command)
        cursor.close()
        conn.commit()


def insert_test_data(db_fullfilename: str):
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        values = [
            ("url1", dt.datetime.now(dt.UTC).timestamp(), 200, "content1.0", "provenance1.0"),
            ("url1", dt.datetime.now(dt.UTC).timestamp(), 200, "content1.1", "provenance1.1"),
            ("url1", dt.datetime.now(dt.UTC).timestamp(), 200, "content1.2", "provenance1.2"),
            ("url2", dt.datetime.now(dt.UTC).timestamp(), 200, "content2.0", "provenance2.0"),
            ("url2", dt.datetime.now(dt.UTC).timestamp(), 200, "content2.1", "provenance2.1"),
            ("url3", dt.datetime.now(dt.UTC).timestamp(), 200, "content3.0", "provenance3.0"),
        ]
        cursor.executemany("INSERT OR REPLACE INTO robots VALUES (?, ?, ?, ?, ?)", values)
        cursor.close()
        conn.commit()


def print_database(db_fullfilename: str):
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        cursor = conn.cursor()
        # response = cursor.execute("SELECT * FROM robots;").fetchall()
        response = cursor.execute("SELECT * FROM robots LIMIT 3;").fetchall()
        cursor.close()
    print(response)


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
        #
        # # response = cursor.execute(inner_query).fetchall()
        # # response = cursor.execute(group_query).fetchall()
        response = cursor.execute(final_query)
        # # response = cursor.execute("SELECT * FROM robots LIMIT 3;").fetchall()
        # response = cursor.execute("SELECT * FROM latest_robots LIMIT 3;").fetchall()

        cursor.close()


if __name__ == "__main__":
    base_dir = "/commoncrawl_robots"
    test_dir = os.path.join(base_dir, "testdir")
    os.makedirs(test_dir, exist_ok=True)

    test_filename = "abc.db"
    test_fullfilename = os.path.join(test_dir, test_filename)
    initialize_db(test_fullfilename)
    insert_test_data(test_fullfilename)
    print_database(test_fullfilename)
    do_grouping(test_fullfilename)

    print("Done")


# https://docs.python.org/3/library/sqlite3.html
# https://www.sqlitetutorial.net/sqlite-python/creating-database/
# https://www.ionos.com/digitalguide/websites/web-development/sqlite3-python/
import os
import argparse
import logging
import sqlite3
import datetime as dt

import tqdm
import fastwarc

import log_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_config.get_standard_streamhandler())


def is_okay_statusline(statusline: str) -> bool:
    return statusline.startswith("200")


def extract_fast(warc_fullfilename: str) -> tuple:
    ok_codes = [200]

    _, provenance = os.path.split(warc_fullfilename)
    stream = fastwarc.stream_io.GZipStream(fastwarc.stream_io.FileStream(warc_fullfilename, 'rb'))
    record_types = fastwarc.warc.WarcRecordType.response
    func_filter = lambda _: (_ is not None) and \
                            (_.http_headers is not None) and \
                            (_.http_headers.status_code in ok_codes)
    archive_iterator = fastwarc.warc.ArchiveIterator(stream,
                                                     # func_filter=func_filter,
                                                     record_types=record_types,
                                                     parse_http=True)
    for idx, record in enumerate(archive_iterator):
        timestamp = record.record_date
        url = record.headers['WARC-Target-URI']
        status_code = record.http_headers.status_code
        if status_code in ok_codes:
            content = record.reader.read()
        else:
            content = ""

        row = (url, timestamp, status_code, content, provenance)
        yield row
    #     print(row)
    # print("Done")


def initialize_db(db_fullfilename):
    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        conn.execute('PRAGMA journal_mode=OFF;')
        logger.info(f"Opened SQLite database with version {sqlite3.sqlite_version}.")
        cursor = conn.cursor()
        command = """
         CREATE TABLE IF NOT EXISTS robots (
         url TEXT, 
         timestamp TEXT,
         response TEXT, 
         content TEXT,
         provenance TEXT
         );
        """
        cursor.execute(command)

        cursor.execute('PRAGMA journal_mode=OFF;')
        result = cursor.fetchone()  
        logger.info("Journal mode set to:", result[0])
        cursor.close()
        conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--crawl_fullfilename', type=str, required=True)
    parser.add_argument('--out_dir', type=str, required=True)
    parser.add_argument('--db_version', type=str, required=True)
    args = parser.parse_args()

    crawl_fullfilename = args.crawl_fullfilename
    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    db_version = args.db_version

    commit_every = 5

    _, crawl_ident = os.path.split(crawl_fullfilename)
    db_filename = f"{crawl_ident}_v{db_version}.db"
    db_fullfilename = os.path.join(out_dir, db_filename)
    initialize_db(db_fullfilename)
    warc_list = sorted(os.listdir(crawl_fullfilename))
    total_file_num = len(warc_list)
    metadata_filename = f"{crawl_ident}_v{db_version}.txt"
    metadata_dir = os.path.join(out_dir, "metadata")
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_fullfilename = os.path.join(metadata_dir, metadata_filename)

    if os.path.exists(metadata_fullfilename):
        with open(metadata_fullfilename, 'r') as f:
            line = f.readline()
            line_split = line.split(",")
            assert 2 == len(line_split)
            last_file_num = int(line_split[0])
            read_total_file_num = int(line_split[1])
            assert total_file_num == read_total_file_num
            logger.info(f"Found metadata file {last_file_num} / {total_file_num}")
    else:
        last_file_num = 0

    with sqlite3.connect(db_fullfilename, timeout=30.0) as conn:
        for idx, warc_filename in tqdm.tqdm(enumerate(warc_list), total=total_file_num):
            if idx < last_file_num:
                continue
            warc_fullfilename = os.path.join(crawl_fullfilename, warc_filename)
            # values_old = extract(warc_fullfilename)
            values = extract_fast(warc_fullfilename)
            if False:
                tp0 = next(values_old)
                tp1 = next(values)
                print(tp0)
                print(tp1)
                timestamp = tp1[1].timestamp()
                recovered = dt.datetime.fromtimestamp(timestamp, dt.UTC)
            cursor = conn.cursor()
            cursor.executemany("INSERT OR REPLACE INTO robots VALUES (?, ?, ?, ?, ?)", values)
            cursor.close()

            if idx % commit_every == 0:
                conn.commit()

            with open(metadata_fullfilename, 'w') as f:
                line_to_write = f"{idx},{total_file_num}"
                f.write(line_to_write)

    cursor = conn.cursor()
    num_rows = cursor.execute("SELECT COUNT(*) FROM robots;").fetchall()[0][0]

    logger.info(f"Done building {db_fullfilename}")
    logger.info(f"{len(warc_list)} warc files")
    logger.info(f"{num_rows} total records")
    logger.info(f"{int(num_rows / len(warc_list))} records / warc file")
    logger.info("Done")

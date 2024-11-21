import os
import time
import logging
import argparse
import datetime as dt
from typing import List

import analyze_db


def get_files_with_extension(filedir: str, extension: str) -> List[str]:
    return list(filter(lambda _: _.endswith(extension), os.listdir(filedir)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_dir', type=str, required=True)
    parser.add_argument('--version', type=str, required=True)
    args = parser.parse_args()

    database_dir = args.database_dir
    version = args.version

    suffix = f"_v{version}.db"
    database_filenames = get_files_with_extension(database_dir, suffix)  # list(filter(lambda _: _.endswith(".txt"), os.listdir(metadata_dir)))

    metadata_dir = os.path.join(database_dir, "metadata")

    #metadata_filenames = get_files_with_extension(metadata_dir, f"v{version}.txt")
    # metadata_filenames = list(filter(lambda _: _.endswith(".txt"), os.listdir(metadata_dir)))

    done = False
    while not done:
        print("=" * 10 + f"{dt.datetime.utcnow()}" + "=" * 10)
        metadata_filenames = get_files_with_extension(metadata_dir, f"_v{version}.txt")
        metadata_fullfilenames = list(sorted([os.path.join(metadata_dir, _) for _ in metadata_filenames]))
        for idx, metadata_fullfilename in enumerate(metadata_fullfilenames):
            with open(metadata_fullfilename, "r") as f:
                _, filename = os.path.split(metadata_fullfilename)
                line = f.readline()
                try:
                    top, bot = line.split(",")
                    frac_str = f"{int(top) / int(bot):5.3}"
                except:
                    frac_str = ""
            print(f"{idx:3}:   ", filename, line, frac_str)
        time.sleep(1)

        # for database_filename in database_filenames:
        #     db_fullfilename = os.path.join(database_dir, database_filename)
        #     print(db_fullfilename)

        #     num_rows = analyze_db.get_num_rows(db_fullfilename)
        #     print(database_filename, num_rows)

    print("Done")

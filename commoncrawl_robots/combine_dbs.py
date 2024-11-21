import os
import sqlite3
import shutil
import logging
import datetime as dt
import time
from typing import List

import tqdm

import log_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_config.get_standard_streamhandler())


def append_first(fullfilename1: str,
                 fullfilename2: str):
    db1 = sqlite3.connect(fullfilename1)
    db2 = sqlite3.connect(fullfilename2)

    cursor1 = db1.cursor()
    cursor2 = db2.cursor()

    cursor1.execute('SELECT * FROM robots')
    rows1 = cursor1.fetchall()

    cursor2.executemany('INSERT INTO robots VALUES (?, ?, ?, ?, ?)', rows1)
    db2.commit()
    cursor2.close()
    cursor1.close()


def build_combined_fullfilename(input_fullfilenames: List[str]) -> str:
    filenames = [os.path.split(_)[1].replace(".db", "") for _ in input_fullfilenames]
    versions = [_.split("_")[1] for _ in filenames]

    version = versions[0]
    assert all(version == _ for _ in versions)

    idents = [_.split("_")[0] for _ in filenames]
    combined_filename = "|".join(idents) + f"_{version}.db"
    return combined_filename


def get_all_idents():
    all_idents = [
        "CC-MAIN-2024-42",
        "CC-MAIN-2024-38",
        "CC-MAIN-2024-33",
        "CC-MAIN-2024-30",
        "CC-MAIN-2024-26",
        "CC-MAIN-2024-22",
        "CC-MAIN-2024-18",
        "CC-MAIN-2024-10",
        "CC-MAIN-2023-50",
        "CC-MAIN-2023-40",
        "CC-MAIN-2023-23",
        "CC-MAIN-2023-14",
        "CC-MAIN-2023-06",
        "CC-MAIN-2022-49",
        "CC-MAIN-2022-40",
        "CC-MAIN-2022-33",
        "CC-MAIN-2022-27",
        "CC-MAIN-2022-21",
        "CC-MAIN-2022-05",
        "CC-MAIN-2021-49",
        "CC-MAIN-2021-43"
    ]
    return all_idents


def do_everything(input_fullfilenames: List[str], fullfilename_out: str):
    filesizes = [(_, os.stat(_).st_size) for _ in input_fullfilenames]
    sorted_descending_filesizes = list(sorted(filesizes, key=lambda _: _[1], reverse=True))

    print("=" * 80)
    for idx, _ in enumerate(sorted_descending_filesizes):
        print(idx, _)
    print("=" * 80)

    source = sorted_descending_filesizes[0][0]
    dest = fullfilename_out
    running_size = os.stat(fullfilename_out).st_size
    if os.path.exists(fullfilename_out) and (os.stat(fullfilename_out).st_size == sorted_descending_filesizes[0][1]):
        pass
    else:
        logger.info(f"Copying {source} to {dest} to start computation")
        shutil.copyfile(source, dest)
    logger.info(f"Starting iterative copying. Schedule: ")

    logger.info(f"   {fullfilename_out} ({running_size})")
    for idx, filesize_tuple in enumerate(sorted_descending_filesizes[1:]):
        fullfilename_to_append = filesize_tuple[0]
        filesize_to_append = os.stat(fullfilename_to_append).st_size
        #  _, filename_to_append = os.path.split(fullfilename_to_append)
        logger.info(f" + {fullfilename_to_append} ({filesize_to_append})")

    for idx, filesize_tuple in enumerate(sorted_descending_filesizes[1:]):
        filename_to_append = filesize_tuple[0]
        filesize_to_append = os.stat(filename_to_append).st_size
            
        running_size = os.stat(fullfilename_out).st_size
        logger.info(f"Appending {filename_to_append} ({filesize_to_append}) to {fullfilename_out} ({running_size})")
        time.sleep(.2)
        t0 = dt.datetime.now()
        append_first(filename_to_append, fullfilename_out)
        t1 = dt.datetime.now()
        logger.info(f"took = {t1 - t0}")
    

def listdir_suffix(fulldirname: str, suffix: str) -> List[str]:
    return [_ for _ in os.listdir(fulldirname) if _.endswith(suffix)] 


def combine_db_pairs() -> List[str]:
    pass


if __name__ == "__main__":
    version = "5"
    databases_dir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle/databases"
    out_dir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle/out"
    os.makedirs(out_dir, exist_ok=True)

    input_filenames = listdir_suffix(databases_dir, f"_v{version}.db")
    combined_fullfilename = build_combined_fullfilename(input_filenames)
    input_fullfilenames = [os.path.join(databases_dir, _) for _ in input_filenames]
    # fullfilename_out = os.path.join(databases_dir, combined_fullfilename)
    # print(fullfilename_out)
    filename_out = f"everything_v{version}.db"
    fullfilename_out = os.path.join(out_dir, filename_out)

    # if True:
    #     if os.path.exists(fullfilename_out):
    #         os.remove(fullfilename_out)
    do_everything(input_fullfilenames, fullfilename_out)

    # print("Done")


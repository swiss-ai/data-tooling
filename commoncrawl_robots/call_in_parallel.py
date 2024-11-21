import os
import sys
import functools
import subprocess
from multiprocessing import Pool


def mp_kernel_unwrapped(crawl_fullfilename: str, out_dir: str, db_version: str):
    this_dir, _ = os.path.split(__file__)
    build_db = os.path.join(this_dir, "build_db.py")
    # run_call = ["python3", f"{build_db} --crawl_fullfilename={crawl_fullfilename} --out_dir={out_dir} --db_version={db_version}"]
    # subprocess.run(run_call, stdout=subprocess.PIPE)
    system_call = f"python3 {build_db} --crawl_fullfilename={crawl_fullfilename} --out_dir={out_dir} --db_version={db_version}"
    os.system(system_call)


if __name__ == "__main__":
    crawls_dir = "/crawls"
    crawls = os.listdir(crawls_dir)
    crawl_fullfilenames = [os.path.join(crawls_dir, _) for _ in crawls]

    out_dir = "/databases"
    db_version = 1

    mp_kernel = functools.partial(mp_kernel_unwrapped, out_dir=out_dir, db_version=db_version)

    is_debug = True
    # is_debug = False
    if is_debug:
        mp_kernel(crawl_fullfilenames[0])
    else:
        p = Pool()
        p.map(mp_kernel, crawl_fullfilenames)

    print("Done")

    # for crawl_fullfilename in crawl_fullfilenames:
    #     print()




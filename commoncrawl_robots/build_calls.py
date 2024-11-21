import os
import time

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
all_idents = list(sorted(all_idents))


if __name__ == "__main__":
    basedir = "/store/swissai/a06/datasets_raw/commoncrawl_kyle"
    logs_dir = os.path.join(basedir, "logs")
    run_dir = os.path.join(basedir, "run")
    db_version = 4

    for ident in all_idents:
        job_name = f"{ident}".replace("CC-MAIN-", "")
        ident_logs_dir = os.path.join(logs_dir, ident)

        os.makedirs(ident_logs_dir, exist_ok=True)
        output_file = os.path.join(ident_logs_dir, "output_%x_%j.log")
        error_file = os.path.join(ident_logs_dir, "output_%x_%j.err")

        sbatch_file = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=05:59:00
#SBATCH --output={output_file}
#SBATCH --error={error_file}
#SBATCH --no-requeue
#SBATCH --mem=460000
#SBATCH --uenv=prgenv-gnu/24.7:v3

##uenv start --view=default prgenv-gnu/24.7:v3
source ~/localenv312/bin/activate
python3 {basedir}/commoncrawl_robots/build_db.py --crawl_fullfilename={basedir}/{ident} --out_dir={basedir}/databases --db_version={db_version}
        """
        sbatch_fullfilename = os.path.join(run_dir, f"{ident}.sbatch")
        with open(sbatch_fullfilename, "w") as f:
            f.write(sbatch_file)

        os.system(f"sbatch {sbatch_fullfilename}")
        time.sleep(.15)
    print("Done")


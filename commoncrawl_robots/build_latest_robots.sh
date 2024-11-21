#!/bin/bash
#SBATCH --job-name=final_db
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=05:59:00
#SBATCH --output=/store/swissai/a06/datasets_raw/commoncrawl_kyle/logs/final/output_%x_%j.log
#SBATCH --error=/store/swissai/a06/datasets_raw/commoncrawl_kyle/logs/final/output_%x_%j.err
#SBATCH --no-requeue
#SBATCH --mem=460000
#SBATCH --uenv=prgenv-gnu/24.7:v3

python3 /store/swissai/a06/datasets_raw/commoncrawl_kyle/commoncrawl_robots/build_latest_robots.py


#!/bin/bash
#SBATCH --job-name=download_one_crawl_data
#SBATCH --output=../logs/download_one_crawl_dtype_%j.output
#SBATCH --error=../logs/download_one_crawl_dtype_%j.error
#SBATCH --account=project_462000353
#SBATCH --time=18:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1000
#SBATCH --partition=small 

module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12
module load cray-python

# $1 test run t or f
# $2 crawl id
# $3 data_type

srun \
        bash download_crawl.sh \
        f \
        2023-14 \
        duplicates

srun \
        bash download_crawl.sh \
        f \
        2023-14 \
        minhash

srun \
        bash download_crawl.sh \
        f \
        2023-14 \
        quality_signals

srun \
        bash download_crawl.sh \
        f \
        2023-14 \
        document

#!/bin/bash
#SBATCH --job-name=download_one_crawl_data
#SBATCH --output=../logs/download_one_crawl_data_%j.output # Name of stdout output file
#SBATCH --error=../logs/download_one_crawl_data_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=1500
#SBATCH --partition=small 

module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12
module load cray-python

# $1 test run true or else
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
        texts


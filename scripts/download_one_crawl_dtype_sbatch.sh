#!/bin/bash
#SBATCH --job-name=download_one_crawl_data
#SBATCH --output=../logs/download_one_crawl_dtype_%j.output # Name of stdout output file
#SBATCH --error=../logs/download_one_crawl_dtype_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=04:00:00
#SBATCH --ntasks=84
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1000
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
        document


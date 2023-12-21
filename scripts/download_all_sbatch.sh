#!/bin/bash
#SBATCH --job-name=download_all
#SBATCH --output=../logs/download_all_%A_%a.output # Name of stdout output file
#SBATCH --error=../logs/download_all_%A_%a.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=15:00:00
#SBATCH --nodes=84
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=1500
#SBATCH --partition=standard
#SBATCH --array=0-83
module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12
module load cray-python

doc_urls="document-urls.txt"
uniq_crawls=($(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000086/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++'))

# $1 test run true or else
# $2 crawl id
# $3 data_type
srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        duplicates

srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        minhash

srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        quality_signals

 
srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        texts

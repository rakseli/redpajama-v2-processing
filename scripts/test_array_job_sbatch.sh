#!/bin/bash
#SBATCH --job-name=download_one_crawl_data
#SBATCH --output=../logs/array_test_%A_%a.output # Name of stdout output file
#SBATCH --error=../logs/array_test_%A_%a.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks=1                                      
#SBATCH --ntasks-per-node=1              
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=500
#SBATCH --partition=small
#SBATCH --array=0-83

doc_urls="document-urls.txt"
uniq_crawls=($(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000353/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++'))

# $1 test run true or else
# $2 crawl id
# $3 data_type
srun echo "${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} $SLURM_NODEID"
srun echo "${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} $SLURM_NODEID"
srun echo "${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} $SLURM_NODEID"
srun echo "${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} $SLURM_NODEID"

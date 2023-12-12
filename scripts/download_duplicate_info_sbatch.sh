#!/bin/bash
#SBATCH --job-name=download_duplicates
#SBATCH --output=./logs/download_duplicates_%j.output # Name of stdout output file
#SBATCH --error=./logs/download_duplicates_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=04:00:00 
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=1750
#SBATCH --partition=small 

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12

cd /scratch/project_462000086/data/redpajama-v2/duplicates-2023-14
cat /scratch/project_462000086/data/redpajama-v2/2023-14-head-middle-duplicate-urls.txt | parallel -j 8 wget -q -x --cut-dirs 2 {}

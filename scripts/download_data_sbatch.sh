#!/bin/bash
#SBATCH --job-name=download_data
#SBATCH --output=./logs/download_data_%j.output # Name of stdout output file
#SBATCH --error=./logs/download_data_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=33:00:00 #change larger for real run
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

cd /scratch/project_462000086/data/redpajama-v2/texts-2023-14
cat /scratch/project_462000086/data/redpajama-v2/2023-14-head-middle-urls.txt | parallel -j 8 wget -q -x --cut-dirs 2 {}

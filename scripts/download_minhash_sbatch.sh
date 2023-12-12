#!/bin/bash
#SBATCH --job-name=download_minhash
#SBATCH --output=/scratch/project_462000086/data/redpajama-v2/logs/download_minhash_%j.output # Name of stdout output file
#SBATCH --error=/scratch/project_462000086/data/redpajama-v2/logs/download_minhash_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=06:15:00 #downloading takes about 6 hours
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=1750
#SBATCH --partition=small 

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12

cd /scratch/project_462000086/data/redpajama-v2/minhash-2023-14
cat /scratch/project_462000086/data/redpajama-v2/urls/2023-14-head-middle-minhash-urls.txt | parallel -j 8 wget -nc -q -x --cut-dirs 2 {}

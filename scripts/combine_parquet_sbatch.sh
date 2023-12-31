#!/bin/bash
#SBATCH --job-name=combine_parquet_minhash
#SBATCH --output=../logs/combine_parquet_minhash_%j.output # Name of stdout output file
#SBATCH --error=../logs/combine_parquet_minhash_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=05:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=7
#SBATCH --mem-per-cpu=1500
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/combine_parquet_files.py

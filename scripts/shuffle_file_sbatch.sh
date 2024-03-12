#!/bin/bash
#SBATCH --job-name=shuffle_parquet
#SBATCH --output=../logs/shuffle_parquet_%j.output # Name of stdout output file
#SBATCH --error=../logs/shuffle_parquet_%j.error  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=20:00:00 #
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --mem-per-cpu=2000
#SBATCH --partition=small

module purge

# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/shuffle_dataset.py --lang $1





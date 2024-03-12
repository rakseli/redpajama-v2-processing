#!/bin/bash
#SBATCH --job-name=fr_downsample_parquet
#SBATCH --output=../logs/fr_downsample_parquet_%j.output
#SBATCH --error=../logs/fr_downsample_parquet_%j.error
#SBATCH --account=project_462000444
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4000
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/downsample_parquet.py --lang fr


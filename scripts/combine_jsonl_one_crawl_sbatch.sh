#!/bin/bash
#SBATCH --job-name=combine_jsonl
#SBATCH --output=../logs/combine_jsonl_%j.output
#SBATCH --error=../logs/combine_jsonl_%j.error  
#SBATCH --account=project_462000353
#SBATCH --time=18:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=1000
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/combine_jsonl.py

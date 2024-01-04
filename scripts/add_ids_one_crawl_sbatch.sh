#!/bin/bash
#SBATCH --job-name=add_ids
#SBATCH --output=../logs/add_ids_%j.output # Name of stdout output file
#SBATCH --error=../logs/add_ids_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=00:05:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1000
#SBATCH --partition=debug

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/add_document_ids.py

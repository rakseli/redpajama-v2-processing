#!/bin/bash
#SBATCH --job-name=add_ids_32_cpus
#SBATCH --output=../logs/add_ids_32_cpus_%j.output 
#SBATCH --error=../logs/add_ids_32_cpus_%j.error  
#SBATCH --account=project_462000353
#SBATCH --time=01:20:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=36
#SBATCH --mem-per-cpu=1200
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/add_document_ids.py

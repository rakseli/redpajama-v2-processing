#!/bin/bash
#SBATCH --job-name=fuzzy_dedup_cross_crawl_english
#SBATCH --output=../logs/fuzzy_dedup_cross_crawl_english_%j.output # Name of stdout output file
#SBATCH --error=../logs/fuzzy_dedup_cross_crawl_english_%j.erros  # Name of stderr error file
#SBATCH --account=project_462000444
#SBATCH --time=48:00:00 #72 max time at small
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=8000 #16 000 with 64 max mem avalaible
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/minhashlsh.py --cross_crawl_dedup --strict --lang en 

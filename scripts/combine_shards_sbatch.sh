#!/bin/bash
#SBATCH --job-name=combine_shards
#SBATCH --output=../logs/combine_shards_%j.out # Name of stdout output file
#SBATCH --error=../logs/combine_shards_%j.err  # Name of stderr error file
#SBATCH --account=project_462000444
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/combine_lang.py --lang $1
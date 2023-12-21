#!/bin/bash
#SBATCH --job-name=dedup_test_1000
#SBATCH --output=../logs/dedup_test_500_files_128_cores_%j.output # Name of stdout output file
#SBATCH --error=../logs/dedup_test_500_files_128_cores%j.erros  # Name of stderr error file
#SBATCH --account=project_462000086
#SBATCH --time=03:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=1250
#SBATCH --partition=small

module purge
# singularity setup
CONTAINER="/scratch/project_462000086/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000086/"


srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000086/akselir/redpajama-v2/src/minhashlsh.py --save true

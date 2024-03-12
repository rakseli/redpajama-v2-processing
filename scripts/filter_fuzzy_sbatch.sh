#!/bin/bash
#SBATCH --job-name=filter_fuzzy
#SBATCH --output=../logs/filter_fuzzy_%j.output 
#SBATCH --error=../logs/filter_fuzzy_%j.error
#SBATCH --account=project_462000444
#SBATCH --time=12:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=6250
#SBATCH --partition=small

module purge

# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

echo "Mem per cpu: $SLURM_MEM_PER_CPU"
echo "CPUs per task: $SLURM_CPUS_PER_TASK"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_fuzzy_duplicates.py --lang $1 --lower_target



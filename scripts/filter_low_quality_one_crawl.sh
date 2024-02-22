#!/bin/bash
#SBATCH --job-name=filter_low_quality_crawl_2023-14
#SBATCH --output=../logs/filter_low_quality_crawl_2023-14_%j.output # Name of stdout output file
#SBATCH --error=../logs/filter_low_quality_crawl_2023-14_%j.error  # Name of stderr error file
#SBATCH --account=project_462000444
#SBATCH --time=24:00:00 #
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=3000 #let's use the 6x size of the file and profile the usage later
#SBATCH --partition=small

module purge
module load LUMI/22.12 



# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"


echo "Mem per cpu: $SLURM_MEM_PER_CPU"
echo "CPUs per task: $SLURM_CPUS_PER_TASK"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_low_quality.py --crawl 2023-14

#!/bin/bash
#SBATCH --job-name=filter_low_quality_crawl_2022-40
#SBATCH --output=../logs/filter_low_quality_crawl_2022-40_%j.output
#SBATCH --error=../logs/filter_low_quality_crawl_2022-40_%j.error  
#SBATCH --account=project_462000444
#SBATCH --time=02:00:00 #
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=3000
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
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_low_quality.py --crawl 2022-40 --lang fr --dtype document

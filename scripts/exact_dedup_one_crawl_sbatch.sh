#!/bin/bash
#SBATCH --job-name=exact_dedup_crawl_2017-09
#SBATCH --output=../logs/exact_dedup_crawl_2017-09_%j.output # Name of stdout output file
#SBATCH --error=../logs/exact_dedup_crawl_2017-09_%j.error  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=45:00:00 #
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=7000
#SBATCH --partition=small

module purge
module load LUMI/22.12 
module load parallel/20230322


# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

echo "Exact dedup of crawl 2017-09"
echo "Mem per cpu: $SLURM_MEM_PER_CPU"
echo "CPUs per task: $SLURM_CPUS_PER_TASK"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_exact_duplicates.py --crawl 2017-09

if [ $? -ne 0 ]; then
    echo "Error: srun filtering duplicates failed with non-zero exit code. Exiting."
    exit 1
fi


srun \
        bash gzip_parallel_jsons.sh 2017-09 exact_deduplicated $SLURM_CPUS_PER_TASK



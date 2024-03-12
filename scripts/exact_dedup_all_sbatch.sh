#!/bin/bash
#SBATCH --job-name=exact_dedup_all
#SBATCH --output=../logs/exact_dedup_array_%A_%a.output 
#SBATCH --error=../logs/exact_dedup_array_%A_%a.error  
#SBATCH --account=project_462000353
#SBATCH --time=45:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem-per-cpu=4800
#SBATCH --partition=small
#SBATCH --array=1-83
module purge
module load LUMI/22.12 
module load parallel/20230322

# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

doc_urls="document-urls.txt"
uniq_crawls=($(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000353/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++'))

echo "Starting dedup crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_exact_duplicates.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

if [ $? -ne 0 ]; then
    echo "Error: srun filtering duplicates failed with non-zero exit code. Exiting."
    exit 1
fi

srun \
        bash gzip_parallel_jsons.sh ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} exact_deduplicated $SLURM_CPUS_PER_TASK



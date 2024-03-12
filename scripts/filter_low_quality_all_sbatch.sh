#!/bin/bash
#SBATCH --job-name=it_filter_low_quality_array
#SBATCH --output=../logs/it_filter_low_quality_array_%A_%a.output # Name of stdout output file
#SBATCH --error=../logs/it_filter_low_quality_array_%A_%a.error  # Name of stderr error file
#SBATCH --account=project_462000444
#SBATCH --time=02:00:00 # 
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --mem-per-cpu=4000 #should be enough for all languages
#SBATCH --partition=small
#SBATCH --array=0-83
module purge
module load LUMI/22.12 

# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

doc_urls="document-urls.txt"
uniq_crawls=($(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000353/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++'))

echo "Starting filter crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}"

srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/filter_low_quality.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} 

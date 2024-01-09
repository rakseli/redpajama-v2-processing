#!/bin/bash
#SBATCH --job-name=download_and_combine_all
#SBATCH --output=../logs/download_and_combine_all_%A_%a.output # Name of stdout output file
#SBATCH --error=../logs/download_and_combine_all_%A_%a.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=32:00:00                           # Rough estimate for processing 9 crawls at a time  
#SBATCH --ntasks=1                                # Number of tasks                     
#SBATCH --ntasks-per-node=1                       # Number of tasks per node
#SBATCH --cpus-per-task=32                        # N cpus
#SBATCH --mem-per-cpu=1200                        
#SBATCH --partition=small
#SBATCH --array=0-8                            #Download and Process 9 crawls at a time use 7-83%9  
module purge
module load LUMI/22.12 
module load parallel/20230322
module load wget/1.21.3-cpeAOCC-22.12
module load cray-python

doc_urls="document-urls.txt"
uniq_crawls=($(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000353/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++'))
echo "Starting to download and process crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}"
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"

# $1 test run true or else
# $2 crawl id
# $3 data_type

#runtime ~3.5h
srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        duplicates

#runtime ~9h
srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        minhash

#runtime ~2.5h
srun  \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        quality_signals

#runtime ~4.5h
srun  \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        document


#runtime ~1.5h
srun singularity exec \
        -B "$SING_BIND" \
        "$CONTAINER" \
        python /scratch/project_462000353/akselir/redpajama-v2/src/add_document_ids.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

#runtime ~5h
srun singularity exec \
        -B "$SING_BIND" \
        "$CONTAINER" \
         python /scratch/project_462000353/akselir/redpajama-v2/src/combine_parquet.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

#runtime guess 6h?
srun singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/combine_jsonl.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

#remove sharded data
srun rm -rf /scratch/project_462000353/data/redpajama-v2/full_data/${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}/{document,document_with_ids,duplicates,minhash,quality_signals}
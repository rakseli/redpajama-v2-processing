#!/bin/bash
#SBATCH --job-name=download_and_combine_all
#SBATCH --output=../logs/download_and_combine_all_%A_%a.output # Name of stdout output file
#SBATCH --error=../logs/download_and_combine_all_%A_%a.erros  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=24:00:00                           # Rough estimate for processing 7 crawls at a time, there is variation due different crawl sizes  
#SBATCH --ntasks=1                                # Number of tasks                     
#SBATCH --ntasks-per-node=1                       # Number of tasks per node
#SBATCH --cpus-per-task=24                        # N cpus, max 32 used, combinations use only 10 cpu. definitely waste here but done for sake of simplicity
#SBATCH --mem-per-cpu=1900                        # Was enough for crawls 0-8
#SBATCH --partition=small
#SBATCH --array=9-83%7                            #Download and Process 7 crawls at a time use 9-83%7  
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

if [ $? -ne 0 ]; then
    echo "Error: srun downloading duplicates failed with non-zero exit code. Exiting."
    exit 1
fi

#runtime ~9h
srun \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        minhash

if [ $? -ne 0 ]; then
    echo "Error: srun downloading minhash failed with non-zero exit code. Exiting."
    exit 1
fi

#runtime ~2.5h
srun  \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        quality_signals

if [ $? -ne 0 ]; then
    echo "Error: srun downloading quality signals failed with non-zero exit code. Exiting."
    exit 1
fi

#runtime ~4.5h
srun  \
        bash download_crawl.sh \
        f \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]} \
        document

if [ $? -ne 0 ]; then
    echo "Error: srun downloading documents failed with non-zero exit code. Exiting."
    exit 1
fi

#add document ids
#runtime ~1.5h
srun singularity exec \
        -B "$SING_BIND" \
        "$CONTAINER" \
        python /scratch/project_462000353/akselir/redpajama-v2/src/add_document_ids.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

if [ $? -ne 0 ]; then
    echo "Error: srun of adding ids failed with non-zero exit code. Exiting."
    exit 1
fi

#combine parquet files
#runtime ~6h
srun singularity exec \
        -B "$SING_BIND" \
        "$CONTAINER" \
         python /scratch/project_462000353/akselir/redpajama-v2/src/combine_parquet.py --crawl ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}


if [ $? -ne 0 ]; then
    echo "Error: srun combining parquet files failed with non-zero exit code. Exiting."
    exit 1
fi


#combine jsonl files
#runtime for 3T docs was ~3,5h
srun \
        bash combine_jsonl.sh \
        ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}

if [ $? -ne 0 ]; then
    echo "Error: srun combining jsonl files failed with non-zero exit code. Exiting."
    exit 1
fi

#finally remove unnecessary files after succesful combination
#runtime guess 1h?
srun \
    bash all_combined.sh \
    ${uniq_crawls[${SLURM_ARRAY_TASK_ID}]}


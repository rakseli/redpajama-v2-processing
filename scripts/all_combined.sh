#!/bin/bash
start_time=$(date +%s)

crawl=$1

all_combined=$(python /scratch/project_462000353/akselir/redpajama-v2/src/all_downloaded.py --path "/scratch/project_462000353/data/redpajama-v2/full_data/$crawl/combined" --n_urls 20)
if [ "$all_combined" = "true" ] ; then
    echo "All files combined succefully"
    rm -rf /scratch/project_462000353/data/redpajama-v2/full_data/$crawl/{document,document_with_ids,duplicates,minhash,quality_signals}
    rm -rf /scratch/project_462000353/data/redpajama-v2/datasets_cache
    mkdir -p /scratch/project_462000353/data/redpajama-v2/datasets_cache
else 
    echo "Some combinations failed, not deleting sharded files files"
fi

end_time=$(date +%s)
runtime_seconds=$((end_time - start_time))

hours=$((runtime_seconds / 3600))
minutes=$(( (runtime_seconds % 3600) / 60 ))
seconds=$((runtime_seconds % 60))

echo "Deletion time for crawl $crawl shards and datasets cache: $hours hours, $minutes minutes, $seconds seconds"

#!/bin/bash
start_time=$(date +%s)

crawl=$1
f_type=$2
n_procs=$3

parallel_jobs=2
writing_procs=$((n_procs - parallel_jobs))


jsonl_files=$(find "/scratch/project_462000353/data/redpajama-v2/full_data/$crawl/$f_type" -type f -name "*.jsonl")

echo "Starting to compress $crawl $f_type jsonls"
echo "Files:"
for file in $jsonl_files; do
    echo "$file"
done
#combine all jsonl files in parallel processes
parallel -j$parallel_jobs pigz -1 -f -v -p $writing_procs ::: "$(printf "%s\n" "${jsonl_files[@]}")"

end_time=$(date +%s)
runtime_seconds=$((end_time - start_time))

hours=$((runtime_seconds / 3600))
minutes=$(( (runtime_seconds % 3600) / 60 ))
seconds=$((runtime_seconds % 60))

echo "jsonl compression for $crawl $f_type: $hours hours, $minutes minutes, $seconds seconds"

#!/bin/bash
d_type=$1
start_time=$(date +%s)

jsonl_files=$(find "/scratch/project_462000353/data/redpajama-v2/full_data" -type f -name "*$d_type.jsonl")

echo "Starting to combine jsonls"
echo "Files:"
for file in $jsonl_files; do
    echo "$file"
done
#combine all jsonl files in parallel processes
parallel -j84 pigz -1 -f -v -p 2 ::: "$(printf "%s\n" "${jsonl_files[@]}")"

end_time=$(date +%s)
runtime_seconds=$((end_time - start_time))

hours=$((runtime_seconds / 3600))
minutes=$(( (runtime_seconds % 3600) / 60 ))
seconds=$((runtime_seconds % 60))

echo "jsonl compression for all data: $hours hours, $minutes minutes, $seconds seconds"


#!/bin/bash
crawl="$1"
d_type="$2"
lang="$3"
output_path="/scratch/project_462000353/data/redpajama-v2/full_data/${crawl}/combined"

if [ "$d_type" = "document" ] ; then
    dir_path="/scratch/project_462000353/data/redpajama-v2/full_data/${crawl}/document_with_ids"
else
    dir_path="/scratch/project_462000353/data/redpajama-v2/full_data/${crawl}/${d_type}"
fi

echo "$output_path"
echo "$dir_path"

mkdir -p "$output_path"
# Navigate to the directory
cd "$dir_path"

# Find all JSONL files in subdirectories and concatenate them into a single file
# en_head.signals.json
# en_head.jsonl

if [ "$d_type" = "quality_signals" ] ; then
        output_file="${lang}_${d_type}.jsonl"
        find . -type f -name "${lang}*.signals.json" -exec cat {} + > "$output_path/$output_file"
        echo "Combined jsons into $output_file"

elif [ "$d_type" = "document" ] ; then

    output_file="${lang}_${d_type}.jsonl"
    find . -type f -name "${lang}*.jsonl" -exec cat {} + > "$output_path/$output_file"
    echo "Combined jsons into $output_file"
else
    echo "Error: wrong d_type, $d_type given"
    exit 1
fi



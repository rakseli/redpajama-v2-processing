#!/bin/bash
start_time=$(date +%s)

crawl="$1"
# Parameters and options
lang=("en" "de" "es" "fr" "it")
data_type=("document" "quality_signals")

# Generate combinations
combinations=()
for l in "${lang[@]}"; do
  for d in "${data_type[@]}"; do
    combinations+=("$crawl $d $l")
  done
done

# Find all JSONL files in subdirectories and concatenate them into a single file
# en_head.signals.json
# en_head.jsonl
combine_jsonl(){
    read -r c d_t la <<< "$1"
    output_path="/scratch/project_462000353/data/redpajama-v2/full_data/${c}/combined"
    mkdir -p "$output_path"
    if [ "$d_t" = "document" ] ; then
    dir_path="/scratch/project_462000353/data/redpajama-v2/full_data/${c}/document_with_ids"
    else
    dir_path="/scratch/project_462000353/data/redpajama-v2/full_data/${c}/${d_t}"
    fi
    # Navigate to the directory
    cd "$dir_path"
    if [ "$d_t" = "quality_signals" ] ; then
        output_file="${la}_${d_t}.jsonl"
        find . -type f -name "${la}*.signals.json" -exec cat {} + > "$output_path/$output_file"
        echo "Combined jsons into $output_file"
    elif [ "$d_t" = "document" ] ; then
        output_file="${la}_${d_t}.jsonl"
        find . -type f -name "${la}*.jsonl" -exec cat {} + > "$output_path/$output_file"
        echo "Combined jsons into $output_file"
    else
        echo "Error: wrong d_type, $d_t given"
        exit 1
    fi

}

export -f combine_jsonl
echo "Starting to combine jsons of crawl $crawl..."
#combine all jsonl files in parallel processes
parallel -j10 combine_jsonl ::: "$(printf "%s\n" "${combinations[@]}")"

end_time=$(date +%s)
runtime_seconds=$((end_time - start_time))

hours=$((runtime_seconds / 3600))
minutes=$(( (runtime_seconds % 3600) / 60 ))
seconds=$((runtime_seconds % 60))

echo "jsonl combination time for crawl $crawl: $hours hours, $minutes minutes, $seconds seconds"

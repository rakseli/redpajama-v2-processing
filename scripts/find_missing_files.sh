#!/bin/bash

# Set the root directory
root_directory="/scratch/project_462000353/data/redpajama-v2/full_data"
lang=$1
f="$lang"_document_quality_filtered.jsonl
# Loop through each subdirectory
for subdirectory in "$root_directory"/*/quality_filtered/; do
    # Check if the file exists in the current subdirectory
    
    if [ ! -e "$subdirectory/$f" ]; then
        # Print the path of the subdirectory where the file is not found
        echo "$f not found in: $subdirectory"
    fi
done

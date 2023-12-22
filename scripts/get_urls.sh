#!/bin/bash

#files
minhash_urls="minhash-urls.txt"
doc_urls="document-urls.txt"
q_urls="quality_signals-urls.txt"
dup_urls="duplicates-urls.txt"
output_path="/scratch/project_462000086/data/redpajama-v2/full_data"

echo "Fetching unique crawls..."
uniq_crawls=$(grep -o -E "[0-9]{4}-[0-9]{2}" "/scratch/project_462000086/data/redpajama-v2/urls/$doc_urls" | awk '!seen[$0]++')
crawl_count=$(tr -cd '[:space:]' <<< "$uniq_crawls" | wc -c)
echo "N of uniq crawls: $crawl_count"
mkdir -p $output_path
for crawl in $uniq_crawls
do
    mkdir -p {"$output_path/$crawl/document","$output_path/$crawl/duplicates","$output_path/$crawl/minhash","$output_path/$crawl/quality_signals"}
    grep -E "/$crawl/[0-9]{4}/[a-z]{2}(_head|_middle)" "/scratch/project_462000086/data/redpajama-v2/urls/$doc_urls" > $output_path/$crawl/$crawl-$doc_urls
    grep -E "/$crawl/[0-9]{4}/[a-z]{2}(_head|_middle)" "/scratch/project_462000086/data/redpajama-v2/urls/$minhash_urls" > $output_path/$crawl/$crawl-$minhash_urls
    grep -E "/$crawl/[0-9]{4}/[a-z]{2}(_head|_middle)" "/scratch/project_462000086/data/redpajama-v2//urls/$q_urls" > $output_path/$crawl/$crawl-$q_urls
    grep -E "/$crawl/[0-9]{4}/[a-z]{2}(_head|_middle)" "/scratch/project_462000086/data/redpajama-v2//urls/$dup_urls" > $output_path/$crawl/$crawl-$dup_urls

done

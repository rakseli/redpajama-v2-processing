#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Error: At least one positional argument is required."
    exit 1
fi

# $1 test run true or else
# $2 is the file containing the urls for one crawl data-type
# $3 is the output path
# $4 data type
testing="$1"

if [ "$testing" = "true" ] ; then
    url_file="/scratch/project_462000086/data/redpajama-v2/urls/url_sample.txt"
    output_path="/scratch/project_462000086/data/redpajama-v2/test_dir"
    data_type='duplicates'
elif [ "$testing" = "false" ] ; then 
    url_file="$2"
    output_path="$3"
    data_type="$4"
else
    echo "Error: testing parameter was set wrong $testing givem"
    exit 1
fi

#number of urls should be downloaded
n_urls=$(wc -l "$url_file" | cut -d' ' -f1)
echo "Number of urls: $n_urls"
#how many times failed urls are tried to download
max_tries=5
n_tries=0
# go to output dir
cd "$output_path/$data_type"

download_url(){
    url="$1"
    dir_prefix=$(echo $url | awk -F/ '{print $(NF-1)}')
    wget -q --directory-prefix="$dir_prefix" "$url"
    if [ $? -ne 0 ]; then
        return 1
    fi
    expected_size=$(curl -sI $url | egrep '^content-length' | cut -d' ' -f2)
    file_path=$(echo $url | cut -d'/' -f8-)
    actual_size=$(stat -c "%s" "$file_path")
    if [ "$expected_size" != "$actual_size" ]; then
        rm $file_path
        return 1
    fi
    }

# Export the function so it can be used by parallel processes
export -f download_url

all_downloaded=false

echo "Starting the download..."
while [ "$a_d" != true ]
    do
    if [ "$max_tries" -gt "$n_tries" ]; then
        cat "failed_downloads.txt" | parallel -j 8 download_url
        all_downloaded=$(python /scratch/project_462000086/akselir/redpajama-v2/src/all_downloaded.py --path "$output_path/$data_type" --n_urls "$n_urls")
        if [ "$all_downloaded" = "true" ] ; then
            a_d=true; 
        else 
            a_d=false;
        fi
        ((n_tries++))
        echo "N tries: $n_tries"
    else
        a_d=true;
        echo "Tried $max_tries times to download missing files but didn't succeed, something must be wrong on server or url..."
    fi
    
done

echo "Done"
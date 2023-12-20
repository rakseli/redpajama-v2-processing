#!/bin/bash

# Script is heavily nested:
# Try to download all data - download failed files again until max tries or success
# Re-download corrupted files - download failed files again until max tries or success - see if still corrupted - back to re-download
# Check if at least one positional argument is provided
if [ $# -eq 0 ]; then
    echo "Error: At least one positional argument is required."
    exit 1
fi

# $1 test run true or else
# $2 is the file containing the urls for one crawl data-type
# $3 is the output path

testing="$1"

if [ "$testing" = "true" ] ; then
    url_file="/scratch/project_462000086/data/redpajama-v2/urls/url_sample.txt"
    output_path="/scratch/project_462000086/data/redpajama-v2/test_dir"
    data_type='duplicates'
else 
    url_file="$2"
    output_path="$3"
    data_type="$4"
fi

#number of urls should be downloaded
n_urls=$(wc -l "$url_file" | cut -d' ' -f1)
echo "Number of urls: $n_urls"
#add 3 that all_downloaded will match right number
n_urls=$((n_urls+3))
#how many times failed urls are tried to download
max_tries=5
n_tries=0
# go to output dir
cd "$output_path/$data_type"
#create files to store failed downloads and file size
#hard coded file names can be used as we are in download dir and we want all child processes to write to same files 
touch "failed_downloads.txt"
touch "file_sizes.txt"
touch "corrupted_files.txt"
touch "failed_re_downloads.txt"
touch "file_re_sizes.txt"

download_url(){
    url="$1"
    dir_prefix=$(echo $url | awk -F/ '{print $(NF-1)}')
    wget -q --directory-prefix="$dir_prefix" "$url"
    if [ $? -ne 0 ]; then
        # If wget failed, append the url to the failed_downloads.txt only if not already exist
        grep -xqF -- "$url" "failed_downloads.txt" || echo "$url" >> "failed_downloads.txt"
    fi
    expected_size=$(curl -sI $url | egrep '^content-length' | cut -d' ' -f2)
    file_path=$(echo $url | cut -d'/' -f8-)
    actual_size=$(stat -c "%s" "$file_path")
    echo "$url,$file_path,$expected_size,$actual_size" >> "file_sizes.txt"

}

download_corrupt(){
    url="$1"
    dir_prefix=$(echo $url | awk -F/ '{print $(NF-1)}')
    wget -q --directory-prefix="$dir_prefix" "$url"
    if [ $? -ne 0 ]; then
        # If wget failed, append the url to the failed_re_downloads.txt only if not already exist
        grep -xqF -- "$url" "failed_re_downloads.txt" || echo "$url" >> "failed_re_downloads.txt"
    fi
    expected_size=$(curl -sI $url | egrep '^content-length' | cut -d' ' -f2)
    file_path=$(echo $url | cut -d'/' -f8-)
    actual_size=$(stat -c "%s" "$file_path")
    echo "$url,$file_path,$expected_size,$actual_size" >> "file_sizes.txt"
}



# Export the function so it can be used by parallel processes
export -f download_url
export -f download_corrupt

echo "Starting the download..."
cat $url_file | parallel -j 8 download_url
echo "Download done"
#check if some downloads failed, and download those again which failed
if [ -s "failed_downloads.txt" ]; then
    echo "Some downloads failed, trying to download them again..."
    a_d=false;
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

else
    echo "All downloads for $url_file were succesful"
fi

echo "Checking for corrupted files..."
#check if some downloads failed, and download those again which failed
c_d=false;
n_tries=0
while [ "$c_d" != true ]
do
    if [ "$max_tries" -gt "$n_tries" ]; then
        all_downloaded=$(python /scratch/project_462000086/akselir/redpajama-v2/src/size_match.py --path "$output_path/$data_type/file_sizes.txt")
        if [ "$all_downloaded" = "true" ] ; then
            c_d=true; 
        else 
            c_d=false;
            cat "corrupted_files.txt" | parallel -j 8 download_corrupt
            if [ -s "failed_re_downloads.txt" ];
                  echo "Some downloads failed, trying to download them again..."


        fi
        ((n_tries++))
        echo "N tries: $n_tries"
    else
        a_d=true;
        echo "Tried $max_tries times to download missing files but didn't succeed, something must be wrong on server or url..."
    fi
done



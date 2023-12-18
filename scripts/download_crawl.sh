#!/bin/bash
# $1 is the file containing the urls for one crawl data-type
url_file="$1"
output_path="$2"
# go to output dir
cd $output_path

failed_downloads="failed_downloads.txt"

touch $failed_downloads


download_url(){
    url="$1"
    wget -q -x --cut-dirs=4 "$url"
    if [ $? -ne 0 ]; then
        # If wget failed, append the url to the failed downloads file
        echo "$url" >> "$failed_downloads"
    fi
}

# Export the function so it can be used by parallel processes
export -f download_url

cat $url_file | parallel -j 8 download_url

#check if some downloads failed, and download those again which failed
if [ -s $failed_downloads]; then
    cat $failed_downloads | parallel -j 8 download_url
    #check if file count match, if not loop the script until everyting is obtained
    all_downloaded=$(python /scratch/project_462000086/akselir/redpajama-v2/src/all_downloaded.py --path $output_path)
    if [ "$all_downloaded" = "true" ] ; then
        a_d=true; 
        else 
        a_d=false; 
    fi
    while [!= $a_d]
    do
        cat $failed_downloads | parallel -j 8 download_url
        all_downloaded=$(python /scratch/project_462000086/akselir/redpajama-v2/src/all_downloaded.py --path $output_path)
         if [ "$all_downloaded" = "true" ] ; then
            a_d=true; 
         else 
            a_d=false;
    done

else
    echo "All downloads for $url_file were succesful"
fi
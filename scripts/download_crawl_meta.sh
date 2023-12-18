#!/bin/bash
# $1 is the file containing the urls for one crawl data-type
url_file="$1"
#output_path="$2"
#cd $output_path
touch file_sizes.txt
start_time=`date +%s`

for url in `cat $url_file`; do
    file_size=$(curl -sI $url | egrep '^content-length' | cut -d' ' -f2)
    echo "$url,$file_size" >> file_sizes.txt
done

end_time=`date +%s`
runtime=$((end-start))
echo "Runtime $runtime"
exit 0
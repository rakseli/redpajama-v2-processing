import subprocess
import re
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--test",action='store_true')

dirs_to_put = [
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-36",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-50",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-45",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-25",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-14",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-32",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-50",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-52",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-07",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-27",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-15",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-29",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-24",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-42",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-41",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-33",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-21",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-44",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-18",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-21",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-31",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2023-14",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-18",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-13",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-13",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2023-06",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-23",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-27",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-10",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-48",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-10",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-16",
"/scratch/project_462000353/data/redpajama-v2/final_data",
"/scratch/project_462000353/data/redpajama-v2/downsampled_data",
"/scratch/project_462000353/data/redpajama-v2/cross_crawl_fuzzy_dedup",
"/project_462000353/data/redpajama-v2/parquet_metadata.jsonl",
"/scratch/project_462000353/data/redpajama-v2/README.md"
]

crawl = re.compile("[0-9]{4}-[0-9]{2}")

def put_to_allas(path_str,test=False):
    """put data to allas to three different buckets depending on file path
    if crawl -> redpajama_v2_crawls bucket
    if processed or combined --> redpajama_v2_processed bucket
    if metadata --> redpajama_v2_metadata bucket

    Args:
        path_str (str): file path
    """    
    splitted = path_str.split("/")
    prefix = 'test_' if test else ''
    if Path(path_str).is_dir():
        print(f"{path_str} is a directory")
        print(crawl.match(splitted[-1]))
        if crawl.match(splitted[-1]) is not None:
            print("Crawl found so path will be added to crawls bucket")
            result = subprocess.run(["a-put",path_str,"-b",f"{prefix}redpajama_v2_crawls","--object",splitted[-1]], text=True,stdout=subprocess.PIPE)
            print(result)
        else:
            print("Some proceced data path found, adding to processed bucket")
            result = subprocess.run(["a-put",path_str,"-b",f"{prefix}redpajama_v2_processed","--object",splitted[-1]], text=True,stdout=subprocess.PIPE)
            print(result)
    else:
        print("Path is a file, so adding to metadata bucket")
        result = subprocess.run(["a-put",path_str,"-b",f"{prefix}redpajama_v2_metadata","--object",splitted[-1]], text=True,stdout=subprocess.PIPE)
        print(result)
    
if __name__ == "__main__":
    args = parser.parse_args()
    if args.test:
        dirs_to_put = ['/scratch/project_462000353/data/redpajama-v2/test_dir',
                       '/scratch/project_462000353/data/redpajama-v2/test_file_1.jsonl',
                       '/scratch/project_462000353/data/redpajama-v2/2030-45']
    for d in dirs_to_put:
        put_to_allas(d,test=args.test)

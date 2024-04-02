import subprocess
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--split_index",type=int,help="which split to process, split 0-9 should be given",default=0)
dirs_to_compress = [
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
#"/scratch/project_462000353/data/redpajama-v2/full_data/2017-43",
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
#"/scratch/project_462000353/data/redpajama-v2/full_data/2019-47",
#"/scratch/project_462000353/data/redpajama-v2/full_data/2020-16",
#"/scratch/project_462000353/data/redpajama-v2/final_data",
#"/scratch/project_462000353/data/redpajama-v2/cross_crawl_fuzzy_dedup"
]
if __name__ == "__main__":
    #chain compressions together so that gz decompression doesn't fill the disk and only one folder is decompressed at a time
    #quota allows decompression of ~10 folders at a time
    #sort for to ensure deterministic shards
    args = parser.parse_args()
    dirs_to_compress.sort()
    dirs = np.array_split(dirs_to_compress, 10)
    if args.split_index>9 or args.split_index<0:
        raise IndexError(f"Index shoud be 0<=i<10, i={args.split_index} given")
    shard = list(dirs[args.split_index])
    first = True
    for d in shard:
        if first:
            result = subprocess.run(["sbatch", "../scripts/pzstd_files_sbatch.sh",d], text=True,stdout=subprocess.PIPE)
            if result.returncode == 0:
                output = result.stdout
                job_id = output.split()[3]
                first = False
            else:
                print(f"Failed to run sbatch with param {d}, terminating the whole loop")
                something_failed=True
                exit(1)
        else:
            result = subprocess.run(["sbatch",f"--dependency=afterok:{job_id}","../scripts/pzstd_files_sbatch.sh",d],text=True,stdout=subprocess.PIPE)
            output = result.stdout
            job_id = output.split()[3]
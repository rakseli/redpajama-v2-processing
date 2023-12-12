import datasets
import sys
import argparse
from datasets import load_dataset
from file_helpers import gather_files, custom_file_sort, check_subfolders
from timer import Timer
from tqdm import tqdm
from pathlib import Path

datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser.add_argument("--data_path", type=str, help="single file or dir", default='/scratch/project_462000086/data/redpajama-v2/duplicates-2023-14')
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000086/data/redpajama-v2/datasets_cache")
parser.add_argument("--batch_size",type=int,help="batch size to use for dataset iteration",default=1000)
parser.add_argument("--num_proc",type=str,help="number of processes for deduplication",default=1)
parser.add_argument("--clean_cache",type=str,help="wheter to clean HF cache",default='false')
parser.add_argument("--save",type=str,help="help wheter to save outputs",default='true')
parser.add_argument("--output_dir",type=str,help="where to write deduplicated dataset",default="/scratch/project_462000086/data/redpajama-v2")
args = parser.parse_args()

def load_data(path,cache_dir,file_type,num_proc):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
            print(f"Starting loading {len(data_files)} files...")
        else:
            data_files = path
            print(f"Starting loading 1 file...")
    #use split parameter to obtain Dataset-object
    data = load_dataset(file_type,data_files=data_files,split='train',cache_dir=cache_dir,num_proc=num_proc)
    return data

if __name__ == "__main__":
    duplicate_files = args.data_path
    files = gather_files(duplicate_files)
    print("Missing files in duplicates")
    check_subfolders(args.data_path)
    print("Missing files in texts")
    check_subfolders('/scratch/project_462000086/data/redpajama-v2/texts-2023-14')
    print("Missing files in minhashes")
    check_subfolders('/scratch/project_462000086/data/redpajama-v2/minhash-2023-14')
    print("Missing quality signals")
    check_subfolders('/scratch/project_462000086/data/redpajama-v2/quality-2023-14')
    sys.exit()

    sorted_by_lang = custom_file_sort(files,file_type='duplicates',sort_criteria='lang')
    t = Timer()
    files_to_dedup = sorted_by_lang[10000:10002]
    print(sorted_by_lang[9999])
    print(sorted_by_lang[10000])

    #with t("Load data"):
    #    data = load_data(files_to_dedup,args.cache_dir,'parquet')
    
    #print(data)
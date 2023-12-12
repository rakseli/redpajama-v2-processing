import datasets
import argparse
import json

from minhashlsh import naive_data_collator
from file_helpers import gather_files, custom_file_sort, read_parquet_file
from timer import Timer
from pathlib import Path
from tqdm import tqdm
from datasets import load_dataset
from torch.utils.data import DataLoader

datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser.add_argument("--data_path", type=str, help="single file or dir", default='/scratch/project_462000086/data/redpajama-v2/minhash-2023-14')
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000086/data/redpajama-v2/datasets_cache")
parser.add_argument("--batch_size",type=int,help="batch size to use for dataset iteration",default=10000)
parser.add_argument("--signature",type=str,help="which minhash signature to use",default='signature_sim0.8')
parser.add_argument("--num_proc",type=str,help="number of processes for deduplication",default=1)
parser.add_argument("--testing",type=str,help="data for testing with larger data, func for artificial sample, and false for real run",default='data')
parser.add_argument("--clean_cache",type=str,help="wheter to clean HF cache",default='false')
parser.add_argument("--save",type=str,help="help wheter to save outputs",default='false')
parser.add_argument("--output_dir",type=str,help="where to write deduplicated dataset",default="/scratch/project_462000086/data/redpajama-v2")

args = parser.parse_args()

def load_data(path):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("json",data_files=data_files,split='train',cache_dir=args.cache_dir,streaming=True)
    #data = datasets.Dataset.from_generator(dataset_generator,gen_kwargs={"shards":data_files},num_proc=args.num_proc)
    return data

if __name__ == "__main__":
    
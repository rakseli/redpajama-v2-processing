import datasets
import sys
import os
import json
import argparse
import gc
import pyarrow.parquet as pq
from datasets import load_dataset
from file_helpers import gather_files
from minhashlsh import naive_data_collator
from timer import Timer
from tqdm import tqdm
from pathlib import Path
from torch.utils.data import DataLoader

'''
TODO 
-add crawl logic
-add filtering mechanism
- 

'''
datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data/")
parser.add_argument("--cache_dir", type=str, help="path HF cache dir",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--test",help="whether to test",action='store_true')
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="bloom_deduplicated")
args = parser.parse_args()

def load_text_data(path,cache_dir):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("json",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    return data

def load_duplicate_data(path,cache_dir):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    data = data.select_columns(['id'])
    return data

def load_minhash_data(path,cache_dir):
    """Load minhash data

    Args:
        path (list,str): path to sinle file, dir or list of files 
        cache_dir (str): HF cache dir

    Returns:
        IterableDataset: 
    """    

    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    return data

def create_duplicate_set(id_dataloader):
    id_set = set()
    for b in id_dataloader:
        id_set.update(b['id'])
    return id_set


if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    if len(os.sched_getaffinity(0))<5:
        raise Exception(f"At least 5 CPUs must be available, only {len(os.sched_getaffinity(0))} can be found!")
    full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
    if not os.path.exists(full_output):
        os.mkdir(full_output)
    num_cpus=len(os.sched_getaffinity(0))
    for lang in ["en","de","it","es","fr"]:
        with t(f"{lang} dedup"):
            duplicates = load_duplicate_data(f"{args.path}/{args.crawl}/combined/{lang}_duplicates.parquet",args.cache_dir)
            dataloader_dup= DataLoader(duplicates, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            id_set = create_duplicate_set(dataloader_dup)
            del duplicates
            del dataloader_dup
            documents = load_text_data(f"{args.path}/{args.crawl}/combined/{lang}_document.jsonl",args.cache_dir)
            minhashes = load_minhash_data(f"{args.path}/{args.crawl}/combined/{lang}_minhash.parquet",args.cache_dir)
            documents = documents.map(function=lambda example: example["id"] not in id_set)
            minhashes = minhashes.map(function=lambda example: example["id"] not in id_set)
            dataloader_doc = DataLoader(documents, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            dataloader_min = DataLoader(minhashes,batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            gc.freeze()
            gc.disable()
            with open(f"{full_output}/{lang}_document_bloom_dedup.jsonl", 'w') as jsonl_file:
                for batch in dataloader_doc:
                    for json_object in batch:
                        json_line = json.dumps(json_object,ensure_ascii=False)
                        jsonl_file.write(json_line + '\n')
                        
            schema = minhashes.features.arrow_schema
            writer = pq.ParquetWriter(f"{full_output}/{lang}_minhash_bloom_dedup.parquet", schema)
            for table in dataloader_min:
                writer.write_table(table=table)
   
            writer.close()

        gc.enable()
        gc.collect()

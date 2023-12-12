import datasets
import numpy as np
import argparse
import os
import sys
import gc
import random
import pickle
import multiprocessing as mp
import torch
import json
from union_find import UnionFind
from datasets import load_dataset
from file_helpers import gather_files, custom_file_sort, read_parquet_file
from timer import Timer
from pathlib import Path
from tqdm import tqdm
from collections import defaultdict
from torch.utils.data import DataLoader
# Modification of https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash.py

'''
TODO


'''


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

#ensures that the Union Find data structure
#is not copied to child processes as long as it is not modified
mp.set_start_method("fork", force=True)


def naive_data_collator(batch):
    """Does nothing, only for dataloader to batch samples 
    and not to convert them to tensors
    
    batch (list): list of dicts 
    Returns:
        list: list of dicts
    """    
    return batch

    

def load_data(path,signature):
    signatures = ['signature_sim1.0', 'signature_sim0.9', 'signature_sim0.8', 'signature_sim0.7']
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=args.cache_dir,streaming=True)
    #data = datasets.Dataset.from_generator(dataset_generator,gen_kwargs={"shards":data_files},num_proc=args.num_proc)
    signatures.remove(signature)
    data = data.rename_column(signature, "signature")
    data = data.remove_columns(signatures)
    return data

def cluster_hashes(data):
    #set n_bands based in RedPajama 2 quality annotations table
    # compute clursters with UnionFind
    n_bands = {'signature_sim0.7':14,'signature_sim0.8':9,'signature_sim0.9':5,'signature_sim1.0':1}
    uf = UnionFind()
    hash_tables: list[dict[int, set]] = [defaultdict(set) for _ in range(n_bands[args.signature])]
    
    dataloader = DataLoader(data, batch_size=args.batch_size,num_workers=args.num_proc,collate_fn=naive_data_collator)
    n_samples = 0
    for batch in dataloader:
        n_samples+=len(batch)
        for item in batch:
            #appears that some ids are missing signature?
            if item["signature"] is None:
                continue
            # find the cluster id of every document
            for i, h in enumerate(item["signature"]):
                hash_tables[i][h].add(item["id_int"])
    for table in tqdm(hash_tables, dynamic_ncols=True, desc="Building hash tables..."):
        # cluster: Set[int]
        for cluster in table.values():
            if len(cluster) <= 1:
                continue
            idx = min(cluster)
            for x in cluster:
                uf.union(x, idx)
    return uf,n_samples

def deduplicate(uf_object,ds,output_path=None):
    """Deduplicate the data and save it to disk

    Args:
        uf_object (UnionFind): duplicate information
        ds (IterableDataset): the data

    Returns:
        None:
    """

    # gc manipulations to ensure that uf object is not unneccessarily copied across processes
    def set_clusters(example):
        example["__cluster__"]=uf_object.find(example["id_int"])
        return example

    #Because of Iterable dataset, transformations are done on the fly
    #set the cluster for each sample in dataset
    ds = ds.map(set_clusters)
    #discard every document that is not the parent of a cluster (that means we keep only one document for each cluster of duplicates and unique documents):
    ds = ds.filter(function=lambda example: example["__cluster__"] == example['id_int'])
    #remove signature, not needed anymore
    ds = ds.remove_columns(['signature'])
    dataloader = DataLoader(ds, batch_size=args.batch_size,num_workers=args.num_proc,collate_fn=naive_data_collator)
    gc.freeze()
    gc.disable()
    if args.save == 'true':
        with open(output_path, 'w') as jsonl_file:
            for batch in dataloader:
                for json_object in batch:
                    json_line = json.dumps(json_object,ensure_ascii=False)
                    jsonl_file.write(json_line + '\n')
    else:
        for batch in dataloader:
            for sample in batch:
                print("Example sample")
                print(sample)
                break


    gc.enable()
    gc.collect()
    
    if args.clean_cache == 'true':
        ds.cleanup_cache_files()

    return None
        
if __name__ == "__main__":
    if args.testing == 'data':
        duplicate_files = args.data_path
        files = gather_files(duplicate_files)
        sorted_by_lang = custom_file_sort(files,sort_criteria='lang')
        print(f"Total N of files in crawl:{len(sorted_by_lang)}")
        t = Timer()
        files_to_dedup = sorted_by_lang[10000:10500]
        with t("Load data"):
            data = load_data(files_to_dedup,args.signature)
        print(f"Time data loading: {int(t.elapsed_times.get('Load data', 0))}s OR {int(t.elapsed_times.get('Load data', 0)/60)}m OR {int(t.elapsed_times.get('Load data', 0)/60/60)}h")
        with t("Cluster"):
            hash_clusters,n_samples = cluster_hashes(data)
        print(f"Time clustering: {int(t.elapsed_times.get('Cluster', 0))}s OR {int(t.elapsed_times.get('Cluster', 0)/60)}m OR {int(t.elapsed_times.get('Cluster', 0)/60/60)}h")
        with t("Dedup"):
            deduplicate(hash_clusters,data,output_path=f"{args.output_dir}/deduplicated_test_data/test_dedup.jsonl")
        print(f"Time dedup: {int(t.elapsed_times.get('Dedup', 0))}s OR {int(t.elapsed_times.get('Dedup', 0)/60)}m OR {int(t.elapsed_times.get('Dedup', 0)/60/60)}h")

        with open(f"{args.output_dir}/deduplicated_test_data/test_dedup.jsonl", "r") as f:
            num_dedupped_lines = sum(1 for _ in f)

        print(f"Len before dedup: {len(n_samples)}")
        print(f"Len after dedup: {num_dedupped_lines}")
        
    elif args.testing == 'func':
        from hashlib import sha256
        from datasets import Dataset
        
        def test_data():
            #generate bytes from str
            str_1 = "unique item here"
            str_2 = "duplicate str incoming"
            unique_tokens= {bytes(str_1,"utf-8")}
            duplicate_tokens = {bytes(str_2,"utf-8")}
            #hash the bytes
            hashes_uniq = np.array([sha256(token).digest() for token in unique_tokens]).reshape(len(unique_tokens), 1)
            hashes_dup = np.array([sha256(token).digest() for token in duplicate_tokens]).reshape(len(duplicate_tokens), 1)
            hs_u: list[bytes] = [bytes(hashes_uniq.byteswap().data)]
            hs_d: list[bytes] = [bytes(hashes_dup.byteswap().data)]
            data_list = [{'text':str_1,'id_int':1,'signature':hs_u},{'text':str_2,'id_int':2,'signature':hs_d},{'text':str_2,'id_int':3,'signature':hs_d}]     
            data = Dataset.from_list(data_list)
            return data
        
        def test_dedup(test_data):
            hash_clusters = cluster_hashes(test_data,1)
            deduplited_data = deduplicate(hash_clusters,test_data)
            assert data['signature'][1] == data['signature'][2]
            assert len(deduplited_data)==2
            return deduplited_data
      
        data = test_data()
        dedup_data = test_dedup(data)
        print("Original data contains 1 unique item and 2 duplicates")
        print(data)    
        print("After dedup only two items should be present")
        print(dedup_data['text'])
    
    else:
        print("Run dedup here")
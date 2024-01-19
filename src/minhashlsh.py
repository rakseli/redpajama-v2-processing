import datasets
import os
import numpy as np
import argparse
import gc
import multiprocessing as mp
import torch
import json
from union_find import UnionFind
from datasets import load_dataset
from file_helpers import gather_files, format_duration
from timer import Timer
from pathlib import Path
from tqdm import tqdm
from collections import defaultdict
from torch.utils.data import DataLoader
# Modification of https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash.py

'''
TODO
Transform into full pipeline form that takes crawl id as argument
'''

datasets.disable_caching()

datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser.add_argument("--path", type=str, help="single file or dir", default='/scratch/project_462000353/data/redpajama-v2/full_data')
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--batch_size",type=int,help="batch size to use for dataset iteration",default=10000)
parser.add_argument("--signature",type=str,help="which minhash signature to use",default='signature_sim0.8')
parser.add_argument("--test",help="whether to test",action='store_true')
parser.add_argument("--output_dir",type=str,help="where to write deduplicated dataset",default="fuzzy_dedup")
parser.add_argument("--cross_crawl_dedup",help="whether to do cross crawl dedup",action='store_true')

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


def load_data(path,signature,cache_dir):
    """Load minhash data

    Args:
        path (list,str): path to sinle file, dir or list of files 
        signature (str): signature to be used in dedup
        cache_dir (str): HF cache dir

    Returns:
        IterableDataset: the data in minimal format
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
    data = data.rename_column(signature, "signature")
    data = data.select_columns(['signature','id','id_int'])
    return data

def cluster_hashes(data,batch_size,num_workers,signature):
    """Find clusters for signatures

    Args:
        data (IterableDataset): dataset to be clustered
        batch_size (int): batch size
        num_workers (int): number of processes
        signature (str): signature to define right n of bands

    Returns:
        UnionFind: union find structure
        int: number of samples in data
    """    
    #set n_bands based in RedPajama 2 quality annotations table
    n_bands = {'signature_sim0.7':14,'signature_sim0.8':9,'signature_sim0.9':5,'signature_sim1.0':1}
    uf = UnionFind()
    hash_tables: list[dict[int, set]] = [defaultdict(set) for _ in range(n_bands[signature])]
    dataloader = DataLoader(data, batch_size=batch_size,num_workers=num_workers,collate_fn=naive_data_collator)
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
                
    # compute clursters with UnionFind
    for table in tqdm(hash_tables, dynamic_ncols=True, desc="Building hash tables..."):
        # cluster: Set[int]
        for cluster in table.values():
            if len(cluster) <= 1:
                continue
            idx = min(cluster)
            for x in cluster:
                uf.union(x, idx)
    return uf,n_samples

def deduplicate(uf_object,ds,batch_size,num_proc,output_path):
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
    #use only id as it's needed for dedup later
    ds = ds.select_columns(['id'])
    dataloader = DataLoader(ds, batch_size=batch_size,num_workers=num_proc,collate_fn=naive_data_collator)
    gc.freeze()
    gc.disable()
   
    with open(output_path, 'w') as jsonl_file:
        for batch in dataloader:
            for json_object in batch:
                json_line = json.dumps(json_object,ensure_ascii=False)
                jsonl_file.write(json_line + '\n')
    
    gc.enable()
    gc.collect()
    
    return None
        
if __name__ == "__main__":
    args = parser.parse_args()
    num_cpus=len(os.sched_getaffinity(0))
    if not args.cross_crawl_dedup:
        #dedup crawl per language
        t = Timer()
        out_dir = f"{args.path}/{args.crawl}/{args.output_dir}"
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        for lang in ["en","de","it","es","fr"]:
            print(f"Starting to dedup lang {lang}")
            full_file_path = f"{args.path}/{args.crawl}/quality_filtered/{lang}_minhash_quality_filtered.parquet"
            data = load_data(full_file_path,args.signature,args.cache_dir)
            with t(f"Cluster {lang}"):
                hash_clusters,n_samples = cluster_hashes(data,batch_size=args.batch_size,num_workers=num_cpus,signature=args.signature)
            print(f"Time clustering: {format_duration(int(t.elapsed_times.get(f'Cluster {lang}', 0)))}")
            with t(f"Dedup {lang}"):
                deduplicate(hash_clusters,data,batch_size=args.batch_size,num_proc=num_cpus,output_path=f"{out_dir}/{lang}_sing_{args.signature}_fuzzy_dedup_ids.jsonl")
            print(f"Time dedup: {format_duration(int(t.elapsed_times.get(f'Dedup {lang}', 0)))}")

            with open(f"{out_dir}/{lang}_sing_{args.signature}_fuzzy_dedup_ids.jsonl", "r") as f:
                num_dedupped_lines = sum(1 for _ in f)
            print(f"Len before dedup: {n_samples}")
            print(f"Len after dedup: {num_dedupped_lines}")
            del hash_clusters
    elif args.cross_crawl_dedup:
        t = Timer()
        for lang in ["en","de","it","es","fr"]:
            full_file_path = f"{args.path}/{args.crawl}/quality_filtered/{lang}_minhash_quality_filtered.parquet"
            data = load_data(full_file_path,args.signature,args.cache_dir)
            print(f"Time data loading: {int(t.elapsed_times.get('Load data', 0))}s OR {int(t.elapsed_times.get('Load data', 0)/60)}m OR {int(t.elapsed_times.get('Load data', 0)/60/60)}h")
            with t("Cluster"):
                hash_clusters,n_samples = cluster_hashes(data,batch_size=args.batch_size,num_workers=args.num_proc,signature=args.signature,)
            print(f"Time clustering: {int(t.elapsed_times.get('Cluster', 0))}s OR {int(t.elapsed_times.get('Cluster', 0)/60)}m OR {int(t.elapsed_times.get('Cluster', 0)/60/60)}h")
            with t("Dedup"):
                deduplicate(hash_clusters,data,batch_size=args.batch_size,num_proc=args.num_proc,save=args.save,output_path=f"{args.output_dir}/deduplicated_test_data/test_dedup.jsonl")
            print(f"Time dedup: {int(t.elapsed_times.get('Dedup', 0))}s OR {int(t.elapsed_times.get('Dedup', 0)/60)}m OR {int(t.elapsed_times.get('Dedup', 0)/60/60)}h")

            with open(f"{args.output_dir}/deduplicated_test_data/test_dedup.jsonl", "r") as f:
                num_dedupped_lines = sum(1 for _ in f)

            print(f"Len before dedup: {n_samples}")
            print(f"Len after dedup: {num_dedupped_lines}")
    
    elif args.test:
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
            hash_clusters = cluster_hashes(data=test_data,batch_size=1,num_workers=1,signature=args.signature)
            #def deduplicate(uf_object,ds,batch_size,num_proc,output_path):
            #cluster_hashes(data,batch_size,num_workers,signature)
            print(f"Original data {test_data}")
            assert test_data['signature'][1] == test_data['signature'][2]
            deduplicate(uf_object=hash_clusters,ds=test_data,batch_size=1,num_proc=1,output_path="/scratch/project_462000353/data/redpajama-v2/fuzzy_dedup_test.jsonl")
            print("Dedupped data:")
            with open(f"/scratch/project_462000353/data/redpajama-v2/fuzzy_dedup_test.jsonl", "r") as f:
                lines = f.readlines()
                for l in lines:
                    print(l)
                num_uniq_docs= sum(1 for _ in lines)
            assert len(num_uniq_docs)==2
            
      
        data = test_data()
        print("Original data contains 1 unique item and 2 duplicates")
        test_dedup(data)
       
        
       
        
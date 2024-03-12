import datasets
import os
import numpy as np
import argparse
import gc
import multiprocessing as mp
import logging
import pyarrow.parquet as pq
import pyarrow as pa
from union_find import UnionFind
from datetime import datetime
from datasets import load_dataset
from file_helpers import gather_files, format_duration
from timer import Timer
from pathlib import Path
from collections import defaultdict
from torch.utils.data import DataLoader
# Modification of https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash.py
#ensures that the Union Find data structure
#is not copied to child processes as long as it is not modified
mp.set_start_method("fork", force=True)

datasets.logging.set_verbosity_debug()
datasets.disable_caching()

parser = argparse.ArgumentParser()
parser.add_argument("--logging_path", type=str, help="single file or dir", default='/scratch/project_462000353/akselir/redpajama-v2/fuzzy_dedup_logs')
parser.add_argument("--path", type=str, help="single file or dir", default='/scratch/project_462000353/data/redpajama-v2/partially_dedupped_iterated')
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--batch_size",type=int,help="batch size to use for dataset iteration",default=10000)
parser.add_argument("--temp_dir",type=str,help="where to write temp files",default="/scratch/project_462000353/data/redpajama-v2/tmp_2")
parser.add_argument("--lang",type=str,help="what language to do cross crawl dedup",default="en")
parser.add_argument("--shard",type=int,help="shard number",default=0)
parser.add_argument("--iteration",type=int,help="iteration of dedup",default=0)
parser.add_argument("--signature",type=str,help="which minhash signature to use",default='signature_sim0.8')

def naive_data_collator(batch):
    """Does nothing, only for dataloader to batch samples 
    and not to convert them to tensors
    
    batch (list): list of dicts 
    Returns:
        list: list of dicts
    """    
    return batch


def load_data(path,cache_dir):
    """Load minhash data

    Args:
        path (list,str): path to sinle file, dir or list of files 
        signature (str): signature to be used in dedup
        cache_dir (str): HF cache dir

    Returns:
        IterableDataset: the data in minimal format
    """    

    if isinstance(path,list):
        print(f"Using {len(path)} files in dedup")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path

    data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
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
    logging.info("Starting to find cluster ids for documents")
    for j,batch in enumerate(dataloader):
        logging.debug(f"Adding batch {j}")
        n_samples+=len(batch)
        for item in batch:
            #appears that some ids are missing signature?
            if item["signature"] is None:
                continue
            # find the cluster id of every document
            for i, h in enumerate(item["signature"]):
                hash_tables[i][h].add(item["id_int"])
    logging.info(f"Total n docs added into hash tables: {n_samples}")
    logging.info(f"Starting to cluster the hashes...")      
    # compute clursters with UnionFind
    for k,table in enumerate(hash_tables):
        logging.debug(f"Computing cluster with hastable {k+1}/{len(hash_tables)}")
        # cluster: Set[int]
        for cluster in table.values():
            if len(cluster) <= 1:
                continue
            idx = min(cluster)
            for x in cluster:
                uf.union(x, idx)
        #hack: trying release some space
        hash_tables[k]=None
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
    dataloader = DataLoader(ds, batch_size=batch_size,num_workers=num_proc,collate_fn=naive_data_collator)
    gc.freeze()
    gc.disable()
    schema = pa.schema([('shard_id', pa.string()),('id', pa.string()),('id_int',pa.uint64()),('signature',pa.list_(pa.binary()))])
    writer = pq.ParquetWriter(output_path, schema=schema)
    n_samples=0
    for b in dataloader:
        n_samples+=len(b)
        pa_table = pa.Table.from_pylist(b,schema=schema)
        writer.write_table(table=pa_table)
    if writer:
        writer.close()             
    
    gc.enable()
    gc.collect()
    
    return n_samples



if __name__ == "__main__":
    args = parser.parse_args()
    now = datetime.now()
    now_str = now.strftime('%Y_%m_%d_%H_%M_%S')
    logging.basicConfig(filename=f"{args.logging_path}/round_2_partial_{args.lang}_fuzzy_dedup_debug_log_{now_str}_iteration_{args.iteration}_{args.shard}.log",format='[ %(asctime)s ]: %(message)s', level=logging.DEBUG)
    num_cpus=len(os.sched_getaffinity(0))-1
    logging.info(f"N of CPUs used for data loading and processing: {num_cpus}")
    #dedup corpus per language
    t = Timer()
    if not os.path.exists(args.temp_dir):
        os.mkdir(args.temp_dir)
    lang = args.lang
    logging.info(f"Starting to dedup lang {lang},shard {args.shard}, iteration {args.iteration}")
    all_files = gather_files(args.path)
    if args.iteration == 0:
        signature_files = [x for x in all_files if f"{lang}_minhash_partial_shuffled_three_iterations_shard_" in x]
        #sort for to ensure deterministic shards
        signature_files.sort()
        data_shards = np.array_split(signature_files, 8)
        shard = list(data_shards[args.shard])
        output_file = f"{args.temp_dir}/{lang}_minhash_partial_dedup_shard_{args.shard}.parquet"
        data = load_data(shard,args.cache_dir)
    if args.iteration == 1:
        all_files = gather_files(args.temp_dir)
        signature_files = [x for x in all_files if f"{lang}_minhash_partial_dedup_shard_" in x]
        #sort for to ensure deterministic shards
        signature_files.sort()
        data_shards = np.array_split(signature_files, 4)
        shard = list(data_shards[args.shard])
        output_file = f"{args.temp_dir}/{lang}_minhash_partial_dedup_iteration_{args.iteration}_shard_{args.shard}.parquet"
        data = load_data(shard,args.cache_dir)
    elif args.iteration > 1:
        all_files = gather_files(args.temp_dir)
        signature_files = [x for x in all_files if f"{lang}_minhash_partial_dedup_iteration_{args.iteration-1}" in x]
        #sort for to ensure deterministic shards
        signature_files.sort()
        data_shards = np.array_split(signature_files, 2)
        shard = list(data_shards[args.shard])
        output_file = f"{args.temp_dir}/{lang}_minhash_partial_dedup_iteration_{args.iteration}_shard_{args.shard}.parquet"
        data = load_data(shard,args.cache_dir)
    with t(f"Cluster {args.shard}"):
        hash_clusters,n_samples = cluster_hashes(data,batch_size=args.batch_size,num_workers=num_cpus,signature=args.signature)
    logging.info(f"Time clustering shard {args.shard}: {format_duration(int(t.elapsed_times.get(f'Cluster {args.shard}', 0)))}")
    with t(f"Dedup {args.shard}"):
        n_samples_after_dedup=deduplicate(hash_clusters,data,batch_size=args.batch_size,num_proc=num_cpus,output_path=output_file)
    logging.info(f"Time dedup shard {args.shard}: {format_duration(int(t.elapsed_times.get(f'Dedup {args.shard}', 0)))}")            
    logging.info(f"Len before dedup: {n_samples}")
    logging.info(f"Len after dedup: {n_samples_after_dedup}")
    logging.info(f"Reduction: {100-(n_samples_after_dedup/n_samples)*100:.2f}%")
    
        

       
        
       
        
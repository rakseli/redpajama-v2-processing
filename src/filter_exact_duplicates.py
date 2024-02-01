import datasets
import os
import json
import argparse
import gc
import multiprocessing as mp
import pyarrow.parquet as pq
import pyarrow as pa
from datasets import load_dataset
from file_helpers import format_duration, gather_files, DateTimeEncoder
from minhashlsh import naive_data_collator
from timer import Timer
from pathlib import Path
from torch.utils.data import DataLoader


datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--cache_dir", type=str, help="path HF cache dir",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="exact_deduplicated")
parser.add_argument("--test",action='store_true')


mp.set_start_method("fork", force=True)


def load_iterable_dataset(path,cache_dir,f_type):
    """Load data
    Args:
        path (list,str): path to sinle file, dir or list of files 
        cache_dir (str): HF cache dir
        f_type (str): file type

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
    if f_type == 'document' or f_type == 'quality_signals':
        data = load_dataset("json",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    elif f_type == 'duplicates':
        data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
        data = data.select_columns(['doc_id'])
    elif f_type == 'minhash':
        data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    else:
        raise ValueError()
    
    return data

def create_duplicate_set(id_dataloader):
    id_set = set()
    for b in id_dataloader:
        for i in b:
            id_set.add(i['doc_id'])
    return id_set

def fix_id(example):
    #fix error in doc id "2014-15/1839/de_head.json/0 --> 2014-15/1839/de_head.json.gz/0
    #fix error in doc id "2014-15/1839/de_middle.json/32322 --> 2014-15/1839/de_middle.json.gz/32322
    id = example['id']
    if 'middle' in id:
        fixed_id=id[:27] + '.gz' + id[27:]
    else:
        fixed_id=id[:25] + '.gz' + id[25:]
        
    example['id']=fixed_id
    return example

if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
    if not os.path.exists(full_output):
        os.mkdir(full_output)
    #leave on CPU for writing the files
    num_cpus=len(os.sched_getaffinity(0))-1
    for lang in ["fr","it","de","es","en"]:
        with t(f"{lang} id set"):
            print(f"Starting building {lang} id set")
            duplicates = load_iterable_dataset(f"{args.path}/{args.crawl}/combined/{lang}_duplicates.parquet",args.cache_dir,'duplicates')
            dataloader_dup= DataLoader(duplicates, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            #holding id sets for a language requires at most about 3x the file size mem -> max needed is 51G
            #as the heaviest computation lift is done by map() function, program is executed sequantially by data type
            #parallelization is achieved by processing different crawls to avoid multiple creations of the english id sets
            #creation of id set from 9G duplicates takes 3h
            #dedup for other languages takes some only some minutes 
            id_set = create_duplicate_set(dataloader_dup)
        print(f"Time creating id set: {format_duration(int(t.elapsed_times.get(f'{lang} id set', 0)))}")
        del duplicates
        del dataloader_dup
        gc.disable()
        for d_type in ['document','minhash']:
            if d_type == 'minhash':
                file_ending = 'parquet'
            else:
                file_ending = 'jsonl'
            with t(f"{lang} {d_type} dedup"):
                if d_type == 'minhash':
                    data = load_iterable_dataset(f"{args.path}/{args.crawl}/combined/{lang}_{d_type}.{file_ending}",args.cache_dir,d_type)
                    data = data.filter(function=lambda example: example["id"] not in id_set)
                    dataloader= DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
                    schema = pq.read_schema(f"{args.path}/{args.crawl}/combined/{lang}_{d_type}.{file_ending}")
                    writer = pq.ParquetWriter(f"{full_output}/{lang}_{d_type}_exact_dedup.{file_ending}", schema=schema)
                    for b in dataloader:
                        pa_table = pa.Table.from_pylist(b,schema=schema)
                        writer.write_table(table=pa_table)
                    if writer:
                        writer.close()

      
                elif d_type=='document':
                    data = load_iterable_dataset(f"{args.path}/{args.crawl}/combined/{lang}_{d_type}.{file_ending}.gz",args.cache_dir,d_type)
                    data = data.map(fix_id)
                    data = data.filter(function=lambda example: example["id"] not in id_set)
                    dataloader= DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
                    with open(f"{full_output}/{lang}_{d_type}_exact_dedup.{file_ending}", 'w') as jsonl_file:
                        for batch in dataloader:
                            for json_object in batch:
                                json_line = json.dumps(json_object,cls=DateTimeEncoder,ensure_ascii=False)
                                jsonl_file.write(json_line + '\n')
                        print(f"Time {lang} {d_type} dedup: {format_duration(int(t.elapsed_times.get(f'{lang} {d_type} dedup', 0)))}")
            #original size of file can't obtained from gzip/pigz 
            # The gzip format represents the input size modulo 2^32, 
            # so the --list option reports incorrect uncompressed sizes and compression ratios for uncompressed files 4 GB and larger
            # thus reduction is calculated from gzip files in separate script
            if args.test:
                break
            
        gc.enable()
        gc.collect()
        del id_set
        if args.test:
            break
       

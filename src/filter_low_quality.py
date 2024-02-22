import datasets
import os
import json
import argparse
import gc
import multiprocessing as mp
import pyarrow.parquet as pq
import pyarrow as pa
from file_helpers import format_duration, DateTimeEncoder
from minhashlsh import naive_data_collator
from filter_exact_duplicates import load_iterable_dataset
from timer import Timer
from torch.utils.data import DataLoader

mp.set_start_method("fork", force=True)

datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--cache_dir", type=str, help="path HF cache dir",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--strict",action='store_true',help="wheter to use strict or loose ids")
parser.add_argument("--extreme",action='store_true',help="wheter to use extreme filtering")
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="quality_filtered")
parser.add_argument("--test",action='store_true')



def create_set(id_dataloader):
    id_set = set()
    for b in id_dataloader:
        for i in b:
            id_set.add(i['id'])
    return id_set

if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
    if not os.path.exists(full_output):
        os.mkdir(full_output)
    #leave on CPU for writing the files
    num_cpus=len(os.sched_getaffinity(0))-1
    for lang in ["en","fr","it","de","es"]:
        if args.strict:
            id_file = f"{args.path}/{args.crawl}/quality_filtered_ids/{lang}_quality_filtered_ids_strict.jsonl.gz"
        elif args.extreme:
            id_file = f"{args.path}/{args.crawl}/quality_filtered_ids/{lang}_quality_filtered_ids_strictest.jsonl.gz"
        else:
            id_file = f"{args.path}/{args.crawl}/quality_filtered_ids/{lang}_quality_filtered_ids.jsonl.gz"
        gc.disable()
        with t(f"{lang} id set"):
            print(f"Starting building {lang} id set")
            ids_to_keep = load_iterable_dataset(id_file,args.cache_dir,'quality_signals')
            dataloader_ids= DataLoader(ids_to_keep, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            #holding id sets for a language requires at most about 5x the file size mem -> max needed is 5G needed
            id_set = create_set(dataloader_ids)
        print(f"Time creating id set: {format_duration(int(t.elapsed_times.get(f'{lang} id set', 0)))}")
        del ids_to_keep
        del dataloader_ids
        for d_type in ['document','minhash']:
            if d_type == 'minhash':
                input_file = f"{args.path}/{args.crawl}/exact_deduplicated/{lang}_{d_type}_exact_dedup.parquet"
                if args.strict:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered_strict.parquet"
                elif args.extreme:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered_strictest.parquet"
                else:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered.parquet"
            else:
                input_file = f"{args.path}/{args.crawl}/exact_deduplicated/{lang}_{d_type}_exact_dedup.jsonl.gz"
                if args.strict:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered_strict.jsonl"
                elif args.extreme:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered_strictest.jsonl"
                else:
                    output_file = f"{full_output}/{lang}_{d_type}_quality_filtered.jsonl"
                
            with t(f"{lang} {d_type} quality filter"):
                if d_type == 'minhash':
                    data = load_iterable_dataset(input_file,args.cache_dir,d_type)
                    data = data.filter(function=lambda example: example["id"] in id_set)
                    dataloader= DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
                    schema = pq.read_schema(input_file)
                    writer = pq.ParquetWriter(output_file, schema=schema)
                    for b in dataloader:
                        pa_table = pa.Table.from_pylist(b,schema=schema)
                        writer.write_table(table=pa_table)
                    if writer:
                        writer.close()

                elif d_type=='document':
                    data = load_iterable_dataset(input_file,args.cache_dir,d_type)
                    data = data.filter(function=lambda example: example["id"] in id_set)
                    dataloader= DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
                    with open(output_file, 'w') as jsonl_file:
                        for batch in dataloader:
                            for json_object in batch:
                                json_line = json.dumps(json_object,cls=DateTimeEncoder,ensure_ascii=False)
                                jsonl_file.write(json_line + '\n')
            
            print(f"{lang} {d_type} quality filter: {format_duration(int(t.elapsed_times.get(f'{lang} {d_type} quality filter', 0)))}")
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
       

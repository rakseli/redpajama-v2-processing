import datasets
import os
import json
import argparse
import gc
import multiprocessing as mp
import pyarrow.parquet as pq
import pyarrow as pa
from file_helpers import format_duration, DateTimeEncoder, gather_files
from minhashlsh import naive_data_collator
from filter_exact_duplicates import load_iterable_dataset
from timer import Timer
from torch.utils.data import DataLoader

mp.set_start_method("fork", force=True)

datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()
parser.add_argument("--data_path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--id_path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/cross_crawl_fuzzy_dedup")
parser.add_argument("--cache_dir", type=str, help="path HF cache dir",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--strict",action='store_true',help="wheter to use strict or loose ids")
parser.add_argument("--extreme",action='store_true',help="wheter to use extreme filtering")
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="final_data")
parser.add_argument("--lang",type=str,help="lang",default="it")
parser.add_argument("--all",help="whether to test",action='store_true')
parser.add_argument("--lower_target",help="whether to use lower target files",action='store_true')



def create_set(id_dataloader):
    id_set = set()
    for b in id_dataloader:
        for i in b:
            id_set.add(i['id'])
    return id_set

if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    full_output = f"/scratch/project_462000353/data/redpajama-v2/{args.output_dir}"
    if not os.path.exists(full_output):
        os.mkdir(full_output)
    #leave on CPU for writing the files
    num_cpus=len(os.sched_getaffinity(0))-1
    if args.all:
        langs_iterable = ["en","fr","it","de","es"]
    else:
        langs_iterable = [args.lang]
    for lang in langs_iterable:
        if args.strict:
            id_file = f"{args.id_path}/{lang}_signature_sim0.8_cross_crawl_fuzzy_dedup_ids_strict.jsonl"
        elif args.extreme:
            id_file = f"{args.id_path}/{lang}_signature_sim0.8_cross_crawl_fuzzy_dedup_ids_strictest.jsonl"
        elif args.lower_target:
            id_file = f"{args.id_path}/{lang}_signature_sim0.8_cross_crawl_fuzzy_dedup_ids_lower_target.jsonl"
        else:
            id_file = f"{args.id_path}/{lang}_signature_sim0.8_cross_crawl_fuzzy_dedup_ids.jsonl"
        gc.disable()
        with t(f"{lang} id set"):
            print(f"Starting building {lang} id set")
            ids_to_keep = load_iterable_dataset(id_file,args.cache_dir,'document')
            dataloader_ids= DataLoader(ids_to_keep, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
            #holding id sets for a language requires at most about 5x the file size mem -> max needed is 5G needed
            id_set = create_set(dataloader_ids)
        print(f"Time creating id set: {format_duration(int(t.elapsed_times.get(f'{lang} id set', 0)))}")
        del ids_to_keep
        del dataloader_ids
        all_files = gather_files(args.data_path)
        if args.strict:
            doc_files = [x for x in all_files if f"{lang}_document_quality_filtered_strict.jsonl" in x]
            output_file = f"{full_output}/{lang}_exact_fuzzy_quality_filtered_documents_strict.jsonl"
        elif args.extreme:
            doc_files = [x for x in all_files if f"{lang}_document_quality_filtered_strictest.jsonl" in x]
            output_file = f"{full_output}/{lang}_exact_fuzzy_quality_filtered_documents_strictest.jsonl"
        else:
            if args.lang=='en':
                doc_files = [x for x in all_files if f"{lang}_document_quality_filtered_strict.jsonl" in x]
            else:
                doc_files = [x for x in all_files if f"{lang}_document_quality_filtered.jsonl" in x]
                
            output_file = f"{full_output}/{lang}_exact_fuzzy_quality_filtered_documents.jsonl"
        data = load_iterable_dataset(doc_files,args.cache_dir,'document')
        data = data.filter(function=lambda example: example["id"] in id_set)
        dataloader= DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
        with t(f"{lang} fuzzy filter"):
            with open(output_file, 'w') as jsonl_file:
                for batch in dataloader:
                    for json_object in batch:
                        json_line = json.dumps(json_object,cls=DateTimeEncoder,ensure_ascii=False)
                        jsonl_file.write(json_line + '\n')
        print(f"Time fuzzy filter: {format_duration(int(t.elapsed_times.get(f'{lang} fuzzy filter', 0)))}")
        gc.enable()
        gc.collect()
        del id_set
       

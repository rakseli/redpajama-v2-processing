import datasets
import argparse
import json
import os
import numpy as np 
import multiprocessing
from multiprocessing import Process
from file_helpers import gather_files
from timer import Timer
from pathlib import Path
from tqdm import tqdm
from datasets import load_dataset
from shutil import rmtree

datasets.logging.set_verbosity_error()

datasets.disable_caching()

'''
TODO
Transform into full pipeline form that takes crawl id as argument
'''


parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--cache_dir", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--test",help="wheter to test",action='store_true')
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="test")

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
    data = load_dataset("json",data_files=data_files,cache_dir=cache_dir,split='train')
    data = data.select_columns(['raw_content','url'])
    return data



def add_id(crawl,file_paths,output,cache_dir):
    for f in file_paths:
        text_data = load_text_data(f,cache_dir)
        source_file=list(text_data.info.download_checksums.keys())[0][72:]
        text_data = text_data.map(lambda example,idx: {'id':f"{crawl}/{source_file}/{idx}"}, num_proc=2,with_indices=True)
        if not os.path.exists(f"{output}/{source_file[:72]}"):
            os.mkdir(f"{output}/{source_file[:72]}")

        with open(f"{output}/{source_file[:-3]}l", 'w') as out_file:
            for l in text_data:
                json.dump(l, out_file,ensure_ascii=False)
                out_file.write('\n')

        text_data.cleanup_cache_files()

    
if __name__ == "__main__":
    args = parser.parse_args()    
    t = Timer()
    with t(f"Add id"):
        full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
        if not os.path.exists(full_output):
            os.mkdir(full_output)
        
        all_files = gather_files(f"{args.path}/{args.crawl}/document")
        all_files.remove(f"{args.path}/{args.crawl}/document/failed_downloads.txt")
        if args.test:
            add_id(args.crawl,all_files[0],full_output,cache_dir=args.cache_dir)
        else:
            chunks = np.array_split(all_files,multiprocessing.cpu_count())
            procs = []
            print(chunks[0][0])
            for c in chunks:
                p=Process(target=add_id,args=(args.crawl,c,full_output,args.cache_dir))
                procs.append(p)
                p.start()
                break
            for proc in procs:
                proc.join()
    
    rmtree("/scratch/project_462000353/data/redpajama-v2/datasets_cache")
    os.mkdir("/scratch/project_462000353/data/redpajama-v2/datasets_cache")
    print(f"Time adding id: {int(t.elapsed_times.get('Add id', 0))}s OR {int(t.elapsed_times.get('Add id', 0)/60)}m OR {int(t.elapsed_times.get('Add id')/60/60)}h")
    
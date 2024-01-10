import datasets
import argparse
import json
import os
import numpy as np 
from multiprocessing import Process
from file_helpers import gather_files
from timer import Timer
from pathlib import Path
from tqdm import tqdm
from datasets import load_dataset
from datasets.utils.logging import disable_progress_bar
from shutil import rmtree

datasets.logging.set_verbosity_critical()
disable_progress_bar()

datasets.disable_caching()

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--cache_dir", type=str, help="path HF cache dir",default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--crawl",type=str,help="crawl id", default='2014-15')
parser.add_argument("--test",help="whether to test",action='store_true')
parser.add_argument("--output_dir",type=str,help="where to write dataset",default="document_with_ids")

def load_text_data(path,cache_dir):
    """Load document in memory and prune unnecessary cols

    Args:
        path (list,str): path to file(s)
        cache_dir (str): HF cache dir

    Returns:
        Dataset: 
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
    data = load_dataset("json",data_files=data_files,cache_dir=cache_dir,split='train')
    data = data.select_columns(['raw_content','url','date_download'])
    return data



def add_id(crawl,file_paths,output,cache_dir):
    """Add ids to documents based on source file and row number
        and saves the result in desired output path
    Args:
        crawl (str): crawl id
        file_paths (list): list of files
        output (str): output path
        cache_dir (str): HF cache dir
    """    
    for f in tqdm(file_paths,desc="Adding ids to files"):
        text_data = load_text_data(f,cache_dir)
        source_file=list(text_data.info.download_checksums.keys())[0][72:]
        text_data = text_data.map(lambda example,idx: {'id':f"{crawl}/{source_file}/{idx}"}, num_proc=2,with_indices=True)
        if not os.path.exists(f"{output}/{source_file[:4]}"):
            os.mkdir(f"{output}/{source_file[:4]}")

        with open(f"{output}/{source_file}l", 'w') as out_file:
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
        try:
            all_files.remove(f"{args.path}/{args.crawl}/document/failed_downloads.txt")
        except:
            pass
        if args.test:
            add_id(args.crawl,all_files[0],full_output,cache_dir=args.cache_dir)
        else:
            cpu_c=len(os.sched_getaffinity(0))
            print(f"Splitting the job with {cpu_c} CPUs")
            chunks = np.array_split(all_files,cpu_c)
            procs = []
            for c in chunks:
                p=Process(target=add_id,args=(args.crawl,c,full_output,args.cache_dir))
                procs.append(p)
                p.start()
            for proc in procs:
                proc.join()
    try:
        rmtree("/scratch/project_462000353/data/redpajama-v2/datasets_cache")
    except Exception as e:
        print(e)
        pass
    
    if not os.path.exists("/scratch/project_462000353/data/redpajama-v2/datasets_cache"):
        os.mkdir("/scratch/project_462000353/data/redpajama-v2/datasets_cache")
        
    print(f"Time adding id: {int(t.elapsed_times.get('Add id', 0))}s OR {int(t.elapsed_times.get('Add id', 0)/60)}m OR {int(t.elapsed_times.get('Add id')/60/60)}h")
    
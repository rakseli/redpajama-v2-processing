import pyarrow.parquet as pq
import argparse
import os
import multiprocessing
from file_helpers import gather_files
from multiprocessing import Process
from timer import Timer
parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--crawl", type=str, help="path to parent dir of files",default="2023-14")             
parser.add_argument("--d_type", type=str, help="data type to be combined",default='minhash')
parser.add_argument("--output_dir",type=str ,help="output path",default="combined")

def parquet_generator(file_paths):
    for f in file_paths:
        yield pq.read_table(f)

def combine_files(files,output_path,d_type,lang,writer=None):
    for table in parquet_generator(files):
        if writer is None:
            writer = pq.ParquetWriter(f"{output_path}/{lang}_{d_type}.parquet", table.schema)
        writer.write_table(table=table)
    if writer:
        writer.close()
        
if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    with t(f"{args.d_type} combination"):
        if multiprocessing.cpu_count()<5:
            raise Exception(f"At least 5 CPUs must be available, only {multiprocessing.cpu_count()} can be found!")
        full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
        if not os.path.exists(full_output):
            os.mkdir(full_output)
        all_files = gather_files(f"{args.path}/{args.crawl}/{args.d_type}")
        file_dict = {"en":[],"de":[],"it":[],"fr":[],"es":[]}
        for file in all_files:
            if "en_" in file:
                file_dict['en'].append(file)
            elif "de_" in file:
                file_dict['de'].append(file)
            elif "it_" in file:
                file_dict['it'].append(file)
            elif "es_" in file:
                file_dict['es'].append(file)
            elif "fr_" in file:
                file_dict['fr'].append(file)

        procs = []
        for lang in ["en","de","it","es","fr"]:
            p=Process(target=combine_files,args=(file_dict[lang],full_output,args.d_type,lang))
            procs.append(p)
            p.start()

        for proc in procs:
            proc.join()
    

    print(f"Time {args.d_type} combination: {int(t.elapsed_times.get(f'{args.d_type} combination', 0))}s OR {int(t.elapsed_times.get(f'{args.d_type} combination', 0)/60)}m OR {int(t.elapsed_times.get(f'{args.d_type} combination')/60/60)}h")
    
        

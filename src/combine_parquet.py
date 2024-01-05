import pyarrow.parquet as pq
import argparse
import os
from file_helpers import gather_files
from multiprocessing import Process
from timer import Timer

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--crawl", type=str, help="path to parent dir of files",default="2014-15")             
parser.add_argument("--output_dir",type=str ,help="output path",default="combined")

def parquet_generator(file_paths):
    """Yield parquet tables

    Args:
        file_paths (list): list of file paths

    Yields:
        pyarrow.Table: 
    """    
    for f in file_paths:
        yield pq.read_table(f)

def combine_files(files,output_path,d_type,lang,writer=None):
    """Combine same language parquet files into one

    Args:
        files (list): list of files
        output_path (str): parent folder for outputs
        d_type (str): duplicates or minhash
        lang (str): language of data
        writer (pyarrow.parquet.ParquetWriter, optional): Writer for parquet file. Defaults to None.
    """    
    for table in parquet_generator(files):
        if writer is None:
            writer = pq.ParquetWriter(f"{output_path}/{lang}_{d_type}.parquet", table.schema)
        writer.write_table(table=table)
    if writer:
        writer.close()
        
if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    with t("parquet combination"):
        if len(os.sched_getaffinity(0))<5:
            raise Exception(f"At least 5 CPUs must be available, only {len(os.sched_getaffinity(0))} can be found!")
        full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
        if not os.path.exists(full_output):
            os.mkdir(full_output)
        all_min_files = gather_files(f"{args.path}/{args.crawl}/minhash")
        all_dup_files = gather_files(f"{args.path}/{args.crawl}/duplicates")
        try:
            all_min_files.remove(f"{args.path}/{args.crawl}/minhash/failed_downloads.txt")
            all_dup_files.remove(f"{args.path}/{args.crawl}/duplicates/failed_downloads.txt")
        except:
            pass
        
        file_dict_min = {"en":[],"de":[],"it":[],"fr":[],"es":[]}
        file_dict_dup = {"en":[],"de":[],"it":[],"fr":[],"es":[]}

        for file in all_min_files:
            if "en_" in file:
                file_dict_min['en'].append(file)
            elif "de_" in file:
                file_dict_min['de'].append(file)
            elif "it_" in file:
                file_dict_min['it'].append(file)
            elif "es_" in file:
                file_dict_min['es'].append(file)
            elif "fr_" in file:
                file_dict_min['fr'].append(file)
        
        for file in file_dict_dup:
            if "en_" in file:
                file_dict_dup['en'].append(file)
            elif "de_" in file:
                file_dict_dup['de'].append(file)
            elif "it_" in file:
                file_dict_dup['it'].append(file)
            elif "es_" in file:
                file_dict_dup['es'].append(file)
            elif "fr_" in file:
                file_dict_dup['fr'].append(file)

        procs = []
        for lang in ["en","de","it","es","fr"]:
            p_min=Process(target=combine_files,args=(file_dict_min[lang],full_output,'minhash',lang))
            p_dup=Process(target=combine_files,args=(file_dict_dup[lang],full_output,'duplicates',lang))
            procs.append(p_min)
            procs.append(p_dup)
            p_min.start()
            p_dup.start()

        for proc in procs:
            proc.join()
    

    print(f"Parquet combination: {int(t.elapsed_times.get('parquet combination', 0))}s OR {int(t.elapsed_times.get('parquet combination', 0)/60)}m OR {int(t.elapsed_times.get('parquet combination')/60/60)}h")
    
        

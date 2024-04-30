import subprocess
import argparse
import os
import json
from file_helpers import gather_files,format_duration
from filelock import FileLock
from timer import Timer

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/cross_crawl_fuzzy_dedup")
parser.add_argument("--meta_data_dir",type=str,help="path to metadata dir",default="/scratch/project_462000353/data/redpajama-v2")
parser.add_argument("--test",action='store_true')

def count_rows_zstd(input_path,meta_data_dir,test=True):
    # Check if zstd is installed
    files = gather_files(input_path)
    zstd_files = [x for x in files if "jsonl" in x and ".zst" in x]
    meta_datas = []
    if test:
        zstd_files=zstd_files[:3]
    for zstd_file in zstd_files:
        print(f"Starting file {zstd_file}")
        # Count rows using zstd and piping to wc
        process = subprocess.Popen(['zstd', '-cdq', zstd_file], stdout=subprocess.PIPE)
        count = subprocess.check_output(['wc', '-l'], stdin=process.stdout)
        process.wait()
        metadata_dict = {"num_rows":int(count.decode().split()[0]),'source_file':zstd_file}
        meta_datas.append(metadata_dict)
    if not test: 
        metadata_file = os.path.join(meta_data_dir,"parquet_metadata.jsonl")
        metadata_file_lock = os.path.join(meta_data_dir,"parquet_metadata.jsonl.lock")
        lock = FileLock(metadata_file_lock)
        with lock:
            mode = "a" if os.path.exists(metadata_file) else "w"
            with open(metadata_file, mode) as out_file:
                for m in meta_datas:
                    json.dump(m, out_file,ensure_ascii=False)
                    out_file.write('\n')
    else:
        for d in meta_datas:
            print(d)
            
    
if __name__ == '__main__':
    args = parser.parse_args()
    t = Timer()
    with t(f"metadata"):
        count_rows_zstd(args.path,args.meta_data_dir,args.test)
    print(f"Time to harvest meta data from jsonl files {args.path}: {format_duration(int(t.elapsed_times.get('metadata', 0)))}")

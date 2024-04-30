import os
import json
import pyarrow.parquet as pq
import zstandard
import argparse
from timer import Timer
from file_helpers import format_duration, gather_files
from filelock import FileLock

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/test")
parser.add_argument("--meta_data_dir",type=str,help="path to metadata dir",default="/scratch/project_462000353/data/redpajama-v2")

def decompress_and_delete_zstd_parquet_files(input_path,meta_data_dir):
    # Find all zstd compressed Parquet files in the directory
    files = gather_files(input_path)
    zstd_files = [x for x in files if "_minhash" in x and ".zst" in x]
    meta_datas = []
    for zstd_file in zstd_files:
        print(f"Staring with file {zstd_file}",flush=True)
        # Decompress the zstd file
        decompressed_file = zstd_file[:-4]  # Remove the .zst extension
        dctx = zstandard.ZstdDecompressor()
        with open(zstd_file, 'rb') as ifh, open(decompressed_file, 'wb') as ofh:
            dctx.copy_stream(ifh, ofh,read_size=8192, write_size=16384)
        # Read Parquet file metadata
        parquet_metadata = pq.ParquetFile(decompressed_file).metadata
        metadata_dict = {"num_rows":parquet_metadata.num_rows,'source_file':zstd_file}
        meta_datas.append(metadata_dict)
        os.remove(decompressed_file)

    metadata_file = os.path.join(meta_data_dir,"parquet_metadata.jsonl")
    metadata_file_lock = os.path.join(meta_data_dir,"parquet_metadata.jsonl.lock")
    lock = FileLock(metadata_file_lock)
    with lock:
        mode = "a" if os.path.exists(metadata_file) else "w"
        with open(metadata_file, mode) as out_file:
            for m in meta_datas:
                json.dump(m, out_file,ensure_ascii=False)
                out_file.write('\n')
        
if __name__ == '__main__':
    args = parser.parse_args()
    t = Timer()
    with t(f"metadata"):
        decompress_and_delete_zstd_parquet_files(args.path,args.meta_data_dir)
    print(f"Time to harvest meta data from parquet files {args.path}: {format_duration(int(t.elapsed_times.get('metadata', 0)))}")


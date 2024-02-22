import pyarrow as pa
import pyarrow.parquet as pq
from file_helpers import gather_files,format_duration
from timer import Timer
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/tmp")
parser.add_argument("--output_dir",type=str ,help="output path",default="sharded_output")
parser.add_argument("--lang",type=str ,default="it")

if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    out=None
    with t("parquet combination"):
        full_output = f"{args.path}/{args.output_dir}"
        if not os.path.exists(full_output):
            os.mkdir(full_output)
        all_files = gather_files(args.path)
        files_to_combine = [x for x in all_files if f"{args.lang}_minhash_partial_dedup_shard_" in x]
        for j, fn in enumerate(files_to_combine):
            print(f"File: {fn}")
            with pq.ParquetFile(fn) as f:
                for i in range(f.num_row_groups):
                    table = f.read_row_group(i)
                    if out is None:
                        out = pq.ParquetWriter(f"{full_output}/{args.lang}_partial_dedup_combined.parquet", table.schema)
                    out.write_table(table)
        if out is not None:
            out.close()

    print(f"Time parquet combination: {format_duration(int(t.elapsed_times.get('parquet combination', 0)))}")
    
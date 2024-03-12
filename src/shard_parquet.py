import pyarrow.parquet as pq
import math
import os
import argparse
from timer import Timer
from file_helpers import format_duration

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="dir", default="/scratch/project_462000353/data/redpajama-v2/tmp_2/sharded_output")
parser.add_argument("--source_file", type=str, help="name of file", default="four_iterations_partial_dedup_combined_shuffled.parquet")
parser.add_argument("--lang",type=str,default="en")
parser.add_argument("--output_path", type=str, help="output path", default='/scratch/project_462000353/data/redpajama-v2/partially_dedupped_iterated_2')
parser.add_argument("--base_name", type=str, help="base for output", default='minhash_partial_shuffled_four_iterations')

def shard_parquet(input_file, output_dir,base_file_name,num_shards,shard=None):
    # Get metadata of the Parquet file
    parquet_file = pq.ParquetFile(input_file)
    total_row_groups = parquet_file.num_row_groups
    print(f"Metadata {input_file}: {parquet_file.metadata}",flush=True)
    print(f"Total n of row groups in file {input_file}: {total_row_groups}",flush=True)
    # Calculate the number of row groups in each shard
    row_groups_per_shard = math.ceil(total_row_groups / num_shards)
    # Shard the Parquet file
    if shard is not None:
        start_row = shard * row_groups_per_shard
        end_row = min((shard + 1) * row_groups_per_shard, total_row_groups)
        shard_table = parquet_file.read_row_groups(list(range(start_row,end_row)))
        # Write the shard to a new Parquet file
        output_file = os.path.join(output_dir, f"{base_file_name}_shard_{shard}.parquet")
        pq.write_table(shard_table, output_file)
        print(f"Shard {shard + 1}/{num_shards} written to {output_file}")
    else:
        for shard_index in range(num_shards):
            start_row = shard_index * row_groups_per_shard
            end_row = min((shard_index + 1) * row_groups_per_shard, total_row_groups)
            shard_table = parquet_file.read_row_groups(list(range(start_row,end_row)))
            # Write the shard to a new Parquet file
            output_file = os.path.join(output_dir, f"{base_file_name}_shard_{shard_index}.parquet")
            pq.write_table(shard_table, output_file)

            print(f"Shard {shard_index + 1}/{num_shards} written to {output_file}",flush=True)
    

if __name__ == "__main__":
    args = parser.parse_args()
    input_file_path =f"{args.path}/{args.lang}_{args.source_file}"
    base_file_name = f"{args.lang}_{args.base_name}"
    if not os.path.exists(args.output_path):
        os.mkdir(args.output_path)
    num_shards = 127
    t = Timer()
    with t(f"Shard {args.lang}"):
        shard_parquet(input_file_path, args.output_path,base_file_name, num_shards)
    print(f"Time sharding lang {args.lang}: {format_duration(int(t.elapsed_times.get(f'Shard {args.lang}', 0)))}")

import pyarrow.parquet as pq
import math
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--lang",type=str,default="en")

def shard_parquet(input_file, output_dir,base_file_name,num_shards,shard=None):
    # Get metadata of the Parquet file
    parquet_file = pq.ParquetFile(input_file)
    total_row_groups = parquet_file.num_row_groups
    print(f"Total n of row groups in file {input_file}: {total_row_groups}")
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

            print(f"Shard {shard_index + 1}/{num_shards} written to {output_file}")
    

if __name__ == "__main__":
    args = parser.parse_args()
    input_file_path =f"/scratch/project_462000353/data/redpajama-v2/tmp/sharded_output/{args.lang}_partial_dedup_combined.parquet"

    output_directory = "/scratch/project_462000353/data/redpajama-v2/partially_dedupped"
    base_file_name = f"{args.lang}_minhash_partial"
    num_shards = 127

    shard_parquet(input_file_path, output_directory,base_file_name, num_shards)
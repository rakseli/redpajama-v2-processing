import random

import pyarrow as pa
import pyarrow.parquet as pq

from argparse import ArgumentParser
from file_helpers import gather_files


ap = ArgumentParser()
ap.add_argument('--out_dir',default='/scratch/project_462000353/data/redpajama-v2/downsampled_data')
ap.add_argument('--lang')


def downsample(lang,out_dir,n_shards=64):
    ratios = {  "de": 10**12/1831408329763,
                "en": 0.16679635865671472,
                "es": 10**12/1698722578461,
                "fr": 10**12/1543539727843
                }
    all_files=gather_files("/scratch/project_462000353/data/redpajama-v2/full_data")
    ratio = ratios[lang]
    if lang == 'en':
        files = [x for x in all_files if f"{lang}_minhash_quality_filtered_strict.parquet" in x]
    else:
        files = [x for x in all_files if f"{lang}_minhash_quality_filtered.parquet" in x]
    for shard in range(n_shards):
        out_file = f"{out_dir}/{lang}_minhash_downsampled_shard_{shard}.parquet"
        out = None
        for j, fn in enumerate(files):
            print(f"Adding crawl {j + 1}/84")
            print(f"File: {fn}")
            with pq.ParquetFile(fn) as f:
                for i in range(f.num_row_groups):
                    table = f.read_row_group(i)
                    total_rows = table.num_rows
                    rows_per_shard = total_rows // n_shards
                    start_row = shard * rows_per_shard
                    end_row = (shard + 1) * rows_per_shard if shard < n_shards - 1 else total_rows
                    indices = random.sample(range(start_row, end_row), int(ratio * (end_row - start_row)))
                    table = table.take(indices)

                    if out is None:
                        out = pq.ParquetWriter(out_file, table.schema)
                    out.write_table(table)
        if out is not None:
            out.close()


if __name__ == '__main__':
    args = ap.parse_args()
    downsample(args.lang,args.out_dir)

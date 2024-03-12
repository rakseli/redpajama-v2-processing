import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import os
from timer import Timer
from file_helpers import gather_files,format_duration
from minhashlsh import naive_data_collator
from datasets import load_dataset
from torch.utils.data import DataLoader

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="single file or dir", default="/scratch/project_462000353/data/redpajama-v2/tmp_2")
parser.add_argument("--source_file", type=str, help="name of file", default="minhash_partial_dedup_iteration_2_shard")
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000353/data/redpajama-v2/datasets_cache")
parser.add_argument("--lang",type=str,help="what language to do cross crawl dedup",default="en")
parser.add_argument("--output_path", type=str, help="output path", default='/scratch/project_462000353/data/redpajama-v2/tmp_2/sharded_output')
parser.add_argument("--output_file", type=str, help="output file", default='four_iterations_partial_dedup_combined_shuffled.parquet')


if __name__ == "__main__":
    #creates approximately shuffed dataset
    #each 10000 rows are shuffeled and source shard of these rows is random for each buffer
    args = parser.parse_args()
    if not os.path.exists(args.output_path):
        os.mkdir(args.output_path)
    t = Timer()
    num_cpus=len(os.sched_getaffinity(0))-1
    all_files = gather_files(args.path)
    source_files = [ x for x in all_files if f"{args.lang}_{args.source_file}" in x]
    print("Loading data",flush=True)
    data = load_dataset("parquet",data_files=source_files,split='train',cache_dir=args.cache_dir,streaming=True)
    data = data.shuffle(seed=66)
    dataloader = DataLoader(data, batch_size=10000,num_workers=num_cpus,collate_fn=naive_data_collator)
    source_metadata = pq.ParquetFile(source_files[0])
    source_schema = source_metadata.schema_arrow
    writer = pq.ParquetWriter(f"{args.output_path}/{args.lang}_{args.output_file}", schema=source_schema)
    print("Start writing",flush=True)
    with t(f"Writing data {args.lang}"):
        for j,batch in enumerate(dataloader):
            if j % 1000 == 0:
                print(f"Writing batch n={j}",flush=True)
            pa_table = pa.Table.from_pylist(batch,schema=source_schema)
            writer.write_table(table=pa_table)
        if writer:
            writer.close()
    
    print(f"Time Writing data: {format_duration(int(t.elapsed_times.get(f'Writing data {args.lang}', 0)))}",flush=True)

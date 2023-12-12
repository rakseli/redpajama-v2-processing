import gzip
import json
import numpy as np
from tqdm import tqdm
from file_helpers import gather_files
from file_helpers import read_parquet_file
from file_helpers import custom_file_sort

def read_gzip_jsonl_to_list(file_path):
    with gzip.open(file_path,'r') as f:
        jsons = f.readlines()
    json_strs = [i.decode('utf-8') for i in jsons]
    jsons_decoded = [json.loads(i) for i in jsons]
    return jsons_decoded



if __name__ == "__main__":
    file_paths = "/scratch/project_462000086/data/redpajama-v2/texts-2023-14"
    output_path = "/scratch/project_462000086/data/redpajama-v2/texts-2023-14-combined"
    files = gather_files(file_paths)
    #sort by subset
    sorted_items = custom_file_sort(files)
    #split into chunks
    chunks = np.array_split(sorted_items,5000)
    for c in tqdm(chunks):
        chunk_jsons = []
        for f_path in tqdm(c):
            one_file = read_gzip_jsonl_to_list(f_path)
            chunk_jsons.extend(one_file)
        
        with open(f'{output_path}/{c[0][59:63]}.jsonl', 'w') as out_file:
            for l in chunk_jsons:
                json.dump(l, out_file,ensure_ascii=False)
                out_file.write('\n')
        break
    
    ##sanity check
    output_json = gather_files(output_path)
    with open(output_json[0], 'r') as f:
        lines = f.readlines()
    json_line = json.loads(lines[1])
    print(json_line[2])
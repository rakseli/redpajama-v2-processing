import datasets
import sys
import argparse
from datasets import load_dataset
from file_helpers import gather_files, custom_file_sort, check_subfolders
from timer import Timer
from tqdm import tqdm
from pathlib import Path
from torch.utils.data import DataLoader

'''
TODO 
-add crawl logic
-add filtering mechanism
- 

'''
datasets.logging.set_verbosity_error()

parser = argparse.ArgumentParser()

parser.add_argument("--duplicate_path", type=str, help="single file or dir", default='/scratch/project_462000086/data/redpajama-v2/duplicates-2023-14')
parser.add_argument("--text_path", type=str, help="single file or dir", default='/scratch/project_462000086/data/redpajama-v2/texts-2023-14')
parser.add_argument("--lang", type=str, help="which language to filter", default='en')
parser.add_argument("--cache_dir", type=str, help="`cache_dir` in load_dataset", default="/scratch/project_462000086/data/redpajama-v2/datasets_cache")
parser.add_argument("--batch_size",type=int,help="batch size to use for dataset iteration",default=1000)
parser.add_argument("--num_proc",type=str,help="number of processes for deduplication",default=1)
parser.add_argument("--clean_cache",type=str,help="wheter to clean HF cache",default='false')
parser.add_argument("--save",type=str,help="help wheter to save outputs",default='true')
parser.add_argument("--output_dir",type=str,help="where to write deduplicated dataset",default="/scratch/project_462000086/data/redpajama-v2")
args = parser.parse_args()

def load_text_data(path,cache_dir):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("json",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    data = data.select_columns(['raw_content'])
    return data

def load_duplicate_data(path,cache_dir):
    if isinstance(path,list):
        print(f"Starting loading {len(path)} files...")
        data_files = path
    elif isinstance(path,str):
        if Path(path).is_dir():
            data_files = gather_files(path)
        else:
            data_files = path
    #use split parameter to obtain Dataset-object
    data = load_dataset("parquet",data_files=data_files,split='train',cache_dir=cache_dir,streaming=True)
    data = data.select_columns(['raw_content'])
    return data
if __name__ == "__main__":
    args = parser.parse_args()
    text_files = gather_files(args.text_path)
    minhash_files = gather_files(args.minhash_path)
    sorted_by_lang = custom_file_sort(text_files,file_type='text',sort_criteria='lang')
    if args.lang == 'de':
        texts = sorted_by_lang[:9999]
    elif args.lang == 'en':
        texts = sorted_by_lang[10000:19999]
        if args.test == 'true':
            texts = texts[:5]
    elif args.lang == 'es':
        texts = sorted_by_lang[20000:29999]
    elif args.lang == 'fr':
        texts = sorted_by_lang[30000:39999]
    elif args.lang == 'it':
        texts = sorted_by_lang[40000:]
    else:
        raise NotImplementedError(f"Only DE, EN, ES, FR and IT available, {args.lang} given!")
    
    #open correspoding files that ids match
    minhashes = [f"{args.minhash_path}/{i[59:-8]}.minhash.parquet" for i in texts]
    text_data = load_text_data(texts,args.cache_dir)
    id_data = load_id_data(minhashes,args.cache_dir)
    dataloader_text = DataLoader(text_data, batch_size=args.batch_size,num_workers=args.num_proc,collate_fn=naive_data_collator)
    dataloader_id = DataLoader(id_data, batch_size=args.batch_size,num_workers=args.num_proc,collate_fn=naive_data_collator)
    if args.save == 'true' and args.output_dir is not None:
        with open(f"{args.output_dir}/test_dir/{args.lang}_texts_with_ids.jsonl", 'w') as jsonl_file:
            for text_batch,id_batch in zip(dataloader_text,dataloader_id):
                for text_dict,id_dict in zip(text_batch,id_batch):
                    text_dict.update(id_dict)
                    json_line = json.dumps(text_dict,ensure_ascii=False)
                    jsonl_file.write(json_line + '\n')
    else:
        for text_batch,id_batch in zip(dataloader_text,dataloader_id):
            for t,i in zip(text_batch,id_batch):
                print("Example sample")
                print(t,i)
                break
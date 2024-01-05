import os
import json
import gzip
import argparse
from timer import Timer
from file_helpers import gather_files
from multiprocessing import Process

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files",default="/scratch/project_462000353/data/redpajama-v2/full_data")
parser.add_argument("--crawl", type=str, help="path to parent dir of files",default="2014-15")             
parser.add_argument("--output_dir",type=str ,help="output path",default="combined")


def jsonl_generator(file_paths):
    for fi in file_paths:
        if ".gz" in fi:
            with gzip.open(fi,'r') as f:
                jsons = f.readlines()
        else:
            with open(fi,'r') as f:
                jsons = f.readlines()
        
        jsons_decoded = [json.loads(i) for i in jsons]
        yield jsons_decoded

def combine_files(files,output_path,d_type,lang):
    with open(f'{output_path}/{lang}_{d_type}.jsonl', 'w') as out_file:
        for list_of_jsons in jsonl_generator(files):
            for l in list_of_jsons:
                json.dump(l, out_file,ensure_ascii=False)
                out_file.write('\n')
        
if __name__ == "__main__":
    args = parser.parse_args()
    t = Timer()
    with t(f"jsonl combination"):
        if len(os.sched_getaffinity(0))<5:
            raise Exception(f"At least 5 CPUs must be available, only {len(os.sched_getaffinity(0))} can be found!")
        full_output = f"{args.path}/{args.crawl}/{args.output_dir}"
        if not os.path.exists(full_output):
            os.mkdir(full_output)
        all_doc_files = gather_files(f"{args.path}/{args.crawl}/document_with_ids")
        all_q_files = gather_files(f"{args.path}/{args.crawl}/quality_signals")
        try:
            all_q_files.remove(f"{args.path}/{args.crawl}/document_with_ids/failed_downloads.txt")
            all_doc_files.remove(f"{args.path}/{args.crawl}/quality_signals/failed_downloads.txt")
        except:
            pass
        file_dict_doc = {"en":[],"de":[],"it":[],"fr":[],"es":[]}
        file_dict_q = {"en":[],"de":[],"it":[],"fr":[],"es":[]}

        for file in all_doc_files:
            if "en_" in file:
                file_dict_doc['en'].append(file)
            elif "de_" in file:
                file_dict_doc['de'].append(file)
            elif "it_" in file:
                file_dict_doc['it'].append(file)
            elif "es_" in file:
                file_dict_doc['es'].append(file)
            elif "fr_" in file:
                file_dict_doc['fr'].append(file)
        
        for file in all_q_files:
            if "en_" in file:
                file_dict_q['en'].append(file)
            elif "de_" in file:
                file_dict_q['de'].append(file)
            elif "it_" in file:
                file_dict_q['it'].append(file)
            elif "es_" in file:
                file_dict_q['es'].append(file)
            elif "fr_" in file:
                file_dict_q['fr'].append(file)

        procs = []
        for lang in ["en","de","it","es","fr"]:
            p_doc=Process(target=combine_files,args=(file_dict_doc[lang],full_output,'document',lang))
            p_q=Process(target=combine_files,args=(file_dict_q[lang],full_output,'quality_signals',lang))
            procs.append(p_doc)
            procs.append(p_q)
            p_doc.start()
            p_q.start()

        for proc in procs:
            proc.join()
    

    print(f"jsonl combination: {int(t.elapsed_times.get('jsonl combination', 0))}s OR {int(t.elapsed_times.get('jsonl combination', 0)/60)}m OR {int(t.elapsed_times.get('jsonl combination')/60/60)}h")
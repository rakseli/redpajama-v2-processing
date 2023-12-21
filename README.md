# Structure
## Dirs
- `/scripts` &rarr; slurm scripts
- `/src` &rarr; src
## py-scripts
- `add_document_ids.py` &rarr; add document ids to text-files
- `all_downloaded.py` &rarr; check if dir contains 50K files
- `combine_files.py` &rarr; read `jsonl.gz` and combine them into `jsonl`
- `download_sample.py` &rarr; download sample of HF datasets
- `file_helpers.py` &rarr; opening and sorting tools
- `filter_bool_duplicates.py` &rarr; filter bloom filter duplicates
    - module not ready, stopped to do this when realized all files are not downloaded
- `inspect_duplicates.py` &rarr; inspect how duplicates look like  
    - module is not working anymore, only reference how to concatenate the datasets
- `minhashlsh.py` &rarr; defines hash-clusters and keeps only parent of a cluster
- `timer.py` &rarr; simple timer
- `union_find.py` &rarr; [Union Find](https://en.wikipedia.org/wiki/Disjoint-set_data_structure) implementation
## bash-scripts
- `get_urls.sh` &rarr; create separate set of urls for each craw
- `dowload_crawl.sh` &rarr; download files from `txt` file
    - gather urls that download failed
    - loop downloading until everything is downloaded
    - uses crawl number and data type as positional arguments
- `download_data_sbatch.sh` &rarr; script to download one crawl all data types
    - run time 11h &rarr; probably 13 hour reservation enough
## Singularity
- Whole code base can be run with container in `/scratch/project_462000086/akselir/containers/preprocessing_container.sif`
# Resource requirements
## Computing requirements
- Most of data is English &rarr; 14.5B/20.8B docs
- Resources for other languages is calculated just as 50% of resources needed for English
- Minhash calculations based on running with 32 cores and 256G mem while processing 1% of full crawl
    - Processing 500/10000 shards &rarr; 3 hours &rarr; 10000/10000 shards &rarr; Union find has time complexity of inverse Ackermann &rarr;  makes disjoint-set operations practically amortized constant time &rarr; 60 hours
    -  `(256GB/2GB) CPU cores x 60 hours = 7680 CPU-core-hours`
- Rough estimate _(read guess)_ for other processing steps they take 50% of Minhash resources
```
| Task        | CPU-core-h /crawl   | Total resources need|
| ----------- | --------------------|-------------------- |
| MinhashLSH  | 11520               | 967680              |
| Other processing|-                | 483840              |
| ----------------------------------|-------------------- |
| Total                             | 1451520 / ~6500 eur |
```
## Disk and inode requirements
- Whole data about 250TB, 4.2M inodes
- 500TB disk and 50M inodes should be enough to process everything at same time

# Pipeline for full data
- Idea is to process each crawl per language
- These steps should be combined into one pipeline
- `minhashlsh.py` contains building blocks for almost all other modules, it can be used as reference
1. Create directory structure
- Run `get_urls.sh`
2. Download data needed for one crawl
    -  Dowload data connected to one crawl and save into format below
    ```
    ── full_data
    ├── crawl_number
        ├──texts
            ├── 0000
                ├── en_head.json.gz
                ├── ...
                ├── it_middle.json.gz
            ├── 0001
            ...
            ├── 4999
        ├──duplicates
            ├── 0000
                ├── en_head.duplicates.parquet
                ├── ...
                ├── it_middle.duplicates.parquet
            ├── 0001
            ...
            ├── 4999
        ...
        ├──quality_signals
    ```
    -  Total inodes used 200K
3. Add document ids to texts and prune extra cols
    - `add_document_ids.py`
        - takes paths to one crawl ids and texts as input, ouputs jsonl with ids
        - works by language
4. Combine 
4. Remove bloom filter duplicates TODO
    - Load the texts with ids as generator
    - Filter the duplicates based on `id`
    - Export to jsonl &rarr; use `force_ascii=False` and `orient='"records"`
    - Remove duplicates files &rarr; 100K inodes
    - After bloom filter dedup, the data size is reduced roughly about 40%
5. Minhash deduplication
    - Load the bloom filtered texts and keep only the ids
    - Filter the bloom filter duplicates
    - compute clusters and filter &rarr; `minhashlsh.py`
    - remove minhash files &rarr; back at 50K inodes
6. Quality filtering
    - Filter with Amandas scripts
    - save
# Memos from the beginning (probably nothing useful for you here)
- Dataset comes in "raw" form meaning that user must do the quality filtering and deduplication self
    1. Download documents, Bloom filterduplicate ids, Minhash signatures, and quality signals
    2. Filter bloom filter duplicates
    3. Fuzzy deduplication with MinhashLSH
    4. Filter low quality documents with quality measures
- Following processing is done for only latest crawl
## Dowloading data
- Following instructions from https://huggingface.co/datasets/togethercomputer/RedPajama-Data-V2
1. Get list of urls pointing to the text documents
    -  `wget "https://data.together.xyz/redpajama-data-v2/v1.0.0/urls/document-urls.txt" -O "document-urls.txt"`

2.  get list of urls pointing to the quality signals
    - `wget "https://data.together.xyz/redpajama-data-v2/v1.0.0/urls/quality_signals-urls.txt" -O "quality_signals-urls.txt"`

3. get list of urls pointing to the ids of duplicate documents
    - `wget "https://data.together.xyz/redpajama-data-v2/v1.0.0/urls/duplicates-urls.txt" -O "duplicates-urls.txt"`

4.  get list of urls pointing to the minhash signatures
    - `wget "https://data.together.xyz/redpajama-data-v2/v1.0.0/urls/minhash-urls.txt" -O "minhash-urls.txt"`

5. Filter the URLs to only use middle and head buckets and latest crawl
    - `grep -E '/2023-14/[0-9]{4}/[a-z]{2}(_head|_middle)' document-urls.txt > 2023-14-head-middle-urls.txt`
    - repeat for each 


# Reference
- `union_find.py` and `timer.py` unmodified from `text-dedup`
- `minhashlsh.py` is modified version of `text_dedup/minhash.py`
 ```
    @software{chenghao_mou_2023_8364980,
    author       = {Chenghao Mou and
                  Chris Ha and
                  Kenneth Enevoldsen and
                  Peiyuan Liu},
    title        = {ChenghaoMou/text-dedup: Reference Snapshot},
    month        = sep,
    year         = 2023,
    publisher    = {Zenodo},
    version      = {2023.09.20},
    doi          = {10.5281/zenodo.8364980},
    url          = {https://doi.org/10.5281/zenodo.8364980}
    }
```
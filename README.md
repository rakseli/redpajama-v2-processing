# Background
- Dataset comes in "raw" form meaning that user must do the quality filtering and deduplication self
    1. Download documents, Bloom filterduplicate ids, Minhash signatures, and quality signals
    2. Filter bloom filter duplicates
    3. Filter low quality documents with quality measures
    4. Fuzzy deduplication with MinhashLSH

# Pipeline for full data
- Idea is to process each crawl per language

1. Create directory structure
- Run `get_urls.sh` &rarr; result
   
    ```
    ── full_data
    ├── crawl_number
        ├──texts

        ├──duplicates

        ├──quality_signals
    ```
2. Download and combine data
    - `download_and_combine_all_sbatch.sh`
3. Remove bloom filter duplicates

4. Quality filter

5. Minhash deduplication
    - compute clusters and filter &rarr; `minhashlsh.py`

# Files
## Dirs
- `/scripts` &rarr; slurm scripts
- `/src` &rarr; src
## py-scripts
- `add_document_ids.py` &rarr; add document ids to text-files
- `all_downloaded.py` &rarr; check if dir contains N number of files
- `combine_jsonl.py` &rarr; read `jsonl.gz` and combine them into `jsonl` &rarr; this module is slow, `combine_jsonl.sh` is used instead
- `combine_parquet_files.py` &rarr; combines one crawl parquet files
- `download_sample.py` &rarr; download sample of HF datasets
- `file_helpers.py` &rarr; gathering, opening and sorting tools
- `filter_bloom_duplicates.py` &rarr; filter bloom filter duplicates still TODO
- `minhashlsh.py` &rarr; defines hash-clusters and keeps only parent of a cluster
- `timer.py` &rarr; simple timer
- `union_find.py` &rarr; [Union Find](https://en.wikipedia.org/wiki/Disjoint-set_data_structure) implementation
## bash-scripts
- `all_combined.sh`
    - check that file number match and delete shards
- `combine_jsonl.sh`
    - combine jsonl files in 10 parallel processes
- `download_and_combine_all_sbatch.sh`
    - master script to download and process all crawls 7 jobs at a time
- `download_crawl.sh` 
    - loops download until success
    - compares actual and expected file sizes to be sure for successful download
    - works in 16 parallel processes
- `deduplicate_sbatch.sh` &rarr; slurm script for dedup
- `get_urls.sh` &rarr; create separate set of urls for each craw can make folder structure
## Singularity
- Dedup/combination can be run with `/scratch/project_462000353/akselir/containers/preprocessing_container.sif`
# Resource requirements
## Computing requirements
- Most of data is English &rarr; 14.5B/20.8B docs
- Resources for other languages is calculated just as 30% of resources needed for English
- Minhash calculations based on running with 32 cores and 256G mem while processing 1% of full crawl
    - Processing 500/10000 shards &rarr; 3 hours 
    - Union find has time complexity of inverse Ackermann &rarr;  makes disjoint-set operations practically amortized constant time &rarr; 60 hours
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
- Whole data about 250TB, 16.8M inodes
- 500TB disk and 50M inodes should be enough to process everything at same time
- We got only 5M inodes, so data needed to be combined before futher processing 

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
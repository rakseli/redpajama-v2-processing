# Background
- This repo describes deduplication [Redpajama 2](https://www.together.ai/blog/redpajama-data-v2) using [Lumi](https://www.together.ai/blog/redpajama-data-v2) supercomputer
- Dataset comes in "raw" form, meaning that the user must do the quality filtering and deduplication self
    1. Download documents, Bloom filter (exact) duplicate IDs, Minhash signatures, and quality signals
    2. Filter exact duplicates
    3. Filter low-quality documents with quality measures
    4. Fuzzy deduplication with MinhashLSH

# Pipeline for full data
- There have been several changes during the process, and this is a genuine description of the full process
1. Create directory structure
- Run `get_urls.sh` &rarr; 
    ```
    ── full_data
    ├── crawl_number_1
        - {crawl_number}-document-urls.txt    
        - {crawl_number}-minhash-urls.txt  
        - {crawl_number}-duplicates-urls.txt  
        - {crawl_number}-quality_signals-urls.txt
        ├──document

        ├──duplicates

        ├──quality_signals
    ...
    ├── crawl_number_n
        -...
    ```
2. Download and combine data
    - `download_and_combine_all_sbatch.sh`
    1. Download all datatypes
    2. Add missing IDs to documents
    3. Combine different dtype files by language
    4. After combination, remove sharded source files 
    - &rarr; 
    ```
    ── full_data
    ├── crawl_number_1
        - {crawl_number}-document-urls.txt    
        ...
        - {crawl_number}-quality_signals-urls.txt
        ├──combined
             -en_document.jsonl.gz
            - en_duplicates.parquet
            - en_minhash.parquet
            - en_quality_signals.jsonl.gz
            ...
            - it_...
    ...
    ├── crawl_number_n
        -...
    ```
3. Remove bloom filter duplicates
- `exact_dedup_all_sbatch.sh`
    - writing large parquet files was too much for [Lustre](https://docs.lumi-supercomputer.eu/storage/parallel-filesystems/lustre/) &rarr; quality signal files were not filtered
    - after exact dedup, gunzip all JSON files (this was also done separately to other JSONs as I realized the project was running out of memory)
    - &rarr;
  ```
   ── full_data
    ├── crawl_number_1
        - {crawl_number}-document-urls.txt    
        ...
        - {crawl_number}-quality_signals-urls.txt
        ├──combined
             - en_document.jsonl.gz
            ...
            - it_...
        ├──exact_deduplicated
            - en_document_exact_dedup.jsonl.gz
            - en_minhash_exact_dedup.parquet
            ...
            - it_...
    ...
    ├── crawl_number_n
        -...
    ```
4. Quality filter
- Pre-computed metrics (number_of_words, number_of_lines, number_of_characters, language_identification, perplexity, stop_words, special_characters, flagged_words, words_per_line_mean, short_line_ratio, character_repetition10gram, character_repetition5gram, word_repetition, unigram_entropy, lines_end_in_punctuation) were used
- Filtering was based on 4 different thresholds based in _p_-quantiles inspired by Cultura X:
   - 0.05% of the data was used to calculate the distributions for German, French, Italian, and Spanish
   - 0.02% of data was used to calculate the distribution for English
   - lower _p_-th percentile was used for the metrics that favor high values (e.g. number_of_words), while metrics favoring low values (e.g., flagged words) will the upper _p_-th percentile
- Regular
   - $p_1 = 10$
   - $p_3 = 90$
- Strict
   - $p_1=20$
   - $p_3 =80$
- Even stricter
   - $p_1=30$
   - $p_3=70$
- Strictest
   - $p_1 = 40$
   - $p_3 = 60$
- Of these documents and signatures were filtered using:
   - Regular for German, French, Italian, and Spanish
   - Strict for English
   - These filters were chosen based on the goal of 100B tokens for each language
- Full code for quality filtering is available [here](https://github.com/mmanteli/redpajama-v2-filter-2023/))
5. Minhash deduplication
    - English strict filter returned about 1.8B signatures &rarr; would need 7.2TB mem
    - Other languages returned about 600M signatures &rarr; would need about 2.5TB mem
    - LUMI has 8 4TB largemem nodes, so almost all signatures would have fit into memory &rarr; however, largemem nodes have 24-hour time limit, so jobs would not have finished
    - All languages EXCEPT Italian were downsampled to about 500M signatures &rarr; this number was chosen as it produces about 100B tokens per language
        - `downsample_parquet.py`
    - Based on partial corpus deduplication tests, the initial estimate of computation time was about 30h, and following was done:
        - After downsampling, signatures were shared to 127 shards to get better parallelization
            - `shard_parquet.py`   
        - Next, data deduplicated in 200GB shards to fit the data into smaller slurm partition
            - `minhashlsh_partial.py`
        - Next, 200GB shards were combined and shared again into 127 shards and fed into full cross-crawl fuzzy dedup
            - `minhashlsh.py`
        - Jobs were launched using `fuzzy_dedup_job_constructor.py`
    - The partial dedup effort was not enough &rarr; there were still too many signatures and duplicates to complete the jobs in Lumi small partition time limit
    - Next, all languages were iteratively dedupped multiple times to reduce the number of duplicates
        - 3 rounds of $\frac{1}{8},\frac{1}{4},\frac{1}{2}$-corpus dedups
            - `shuffle_dataset.py` to make dedups more effective
            - after shuffling, the file was sharded
        - the effort reduced the corpus size by about ~40%
        - this allowed full deduplication of the target
        -  `minhashlsh_partial.py`
        -  `minhashlsh_partial_round_2.py` &rarr; same module but done because of bad design of first
        - Jobs were launched using `fuzzy_dedup_job_constructor_partial.py`
        - Used 0.2-6 hours per shard with 3-16 CPUs
    - Finally, the duplicates were filtered with `filter_fuzzy_duplicates.py`
        - Used ~30 hours with 32 CPUs
        - Reduction from initial size was about 50%
## Singularity
- Dedup/combination can be run with `/scratch/project_462000353/akselir/containers/preprocessing_container.sif`
# Resource requirements
## Disk and inode requirements
- Whole data about 250TB in compressed format, 16.8M inodes
- 500TB disk and 50M inodes should be enough to process everything at the same time
- We got only 5M inodes, so data needed to be combined before any further processing and later compressed again
- While holding intermediate files, the total disk consumed 369TB
## Computing
- Total (including debugging and failed runs), about 250K/CPUh was used for the effort

# References

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

    @article{Nguyen_Van Nguyen_Lai_Man_Ngo_Dernoncourt_Rossi_Nguyen_2023, title={CulturaX: A Cleaned, Enormous, and Multilingual Dataset for Large Language Models in 167 Languages}, DOI={10.48550/arxiv.2309.09400}, journal={arXiv}, author={Nguyen, Thuat and Van Nguyen, Chien and Lai, Viet Dac and Man, Hieu and Ngo, Nghia Trung and Dernoncourt, Franck and Rossi, Ryan A. and Nguyen, Thien Huu}, year={2023} 
```

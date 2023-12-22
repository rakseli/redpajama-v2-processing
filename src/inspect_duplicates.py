from datasets import load_from_disk, load_dataset, concatenate_datasets

#read the data with cluster info
cluster_data = load_from_disk("/scratch/project_462000353/data/redpajama-v2/clustered_data")
#get source info
source_files = cluster_data.info.download_checksums.keys()
source_files = [f"/scratch/project_462000353/data/redpajama-v2/texts-2023-14/{i[61:-16]}.json.gz" for i in source_files]
text_data = load_dataset("json",data_files=source_files,cache_dir='/scratch/project_462000353/data/redpajama-v2/datasets_cache')
print(text_data)
text_features = list(text_data['train'].features.keys())
text_features.remove('raw_content')
text_data = text_data.remove_columns(text_features)
cluster_features = list(cluster_data.features.keys())
cluster_features.remove('__cluster__')
cluster_features.remove('id')
cluster_data = cluster_data.remove_columns(cluster_features)
combined_data = concatenate_datasets([text_data['train'], cluster_data], axis=1)
df = combined_data.to_pandas()
print(df.head())
#count n of docs in each cluster
counts = df['__cluster__'].value_counts().to_frame()
#filter only clusters with >1 members
duplicates = counts.loc[counts['count']>2]
print(duplicates)
#get doc ids of each cluster
duplicate_docs = {}
duplicate_clusters = duplicates.index.values
for c in duplicate_clusters:
    duplicate_rows = df.loc[df['__cluster__']==c]
    duplicate_rows = duplicate_rows.reset_index()
    print(f"Duplicates with cluster id: {c}")
    for i in range(len(duplicate_rows)):
       print(f"Content: \n",duplicate_rows.iloc[i,1],"\n")
       print(f"id",duplicate_rows.iloc[i,2],"\n")
    print("------------------------")
    

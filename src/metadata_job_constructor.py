import subprocess

dirs_to_compress = [
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-36",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-50",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-45",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-25",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-14",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-32",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-50",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-52",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-07",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-27",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-15",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-29",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-24",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-42",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-35",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-41",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-33",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-17",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-22",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-21",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-44",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-18",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-21",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-31",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2023-14",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-18",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-13",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-34",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-26",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-13",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-30",
"/scratch/project_462000353/data/redpajama-v2/full_data/2016-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2023-06",
"/scratch/project_462000353/data/redpajama-v2/full_data/2014-23",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-43",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-27",
"/scratch/project_462000353/data/redpajama-v2/full_data/2022-49",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-40",
"/scratch/project_462000353/data/redpajama-v2/full_data/2021-10",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-04",
"/scratch/project_462000353/data/redpajama-v2/full_data/2018-09",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-39",
"/scratch/project_462000353/data/redpajama-v2/full_data/2015-48",
"/scratch/project_462000353/data/redpajama-v2/full_data/2017-51",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-10",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-05",
"/scratch/project_462000353/data/redpajama-v2/full_data/2019-47",
"/scratch/project_462000353/data/redpajama-v2/full_data/2020-16",
"/scratch/project_462000353/data/redpajama-v2/downsampled_data",
"/scratch/project_462000353/data/redpajama-v2/partially_dedupped",
"/scratch/project_462000353/data/redpajama-v2/partially_dedupped_iterated",
"/scratch/project_462000353/data/redpajama-v2/partially_dedupped_iterated_2"
]
if __name__ == "__main__":
    #sort for to ensure deterministic shards
    for d in dirs_to_compress:
        result = subprocess.run(["sbatch", "../scripts/get_metadata_signatures_sbatch.sh",d], text=True,stdout=subprocess.PIPE)
        if result.returncode == 0:
            output = result.stdout
            print(output)
        else:
            print(f"Failed to run sbatch with param {d}, terminating the whole loop")
            exit(1)
       
#!/bin/bash
#SBATCH --output=../logs/pzstd_folder_%j.output
#SBATCH --error=../logs/pzstd_folder_%j.error
#SBATCH --account=project_462000444
#SBATCH --time=18:00:00                           
#SBATCH --ntasks=1                                                   
#SBATCH --ntasks-per-node=1                       
#SBATCH --cpus-per-task=4                        
#SBATCH --mem-per-cpu=500                        
#SBATCH --partition=small

module purge
module load LUMI/22.12 
module load parallel/20230322

start_time=$(date +%s)

file_path=$1
n_procs=$SLURM_CPUS_PER_TASK
writing_procs=2
parallel_jobs=$((n_procs - writing_procs))

gz_files=$(find "$file_path" -type f -name "*.gz")
echo "Starting to decompress gz files $file_path if any"
if [ -z "$gz_files" ]; then
    echo "No .gz files found in $file_path."
else
    parallel -j$parallel_jobs pigz -v -d -p $writing_procs ::: "$(printf "%s\n" "${gz_files[@]}")"
    if [ $? -ne 0 ]; then
        echo "At least one of the parellel operations failed with non zero exit code, stopping the whole job"
        exit 1
    else
        echo "All commands executed successfully"
    fi
fi
wait
#decompress all gz files in parallel processes
echo "Starting to compress with zstd"
pzstd -5 -r --rm -v -p $n_procs $file_path
if [ $? -ne 0 ]; then
    echo "pztds failed with non zero exit code, stopping the whole job"
    exit 1
else
    echo "Done"
    end_time=$(date +%s)
    runtime_seconds=$((end_time - start_time))
    hours=$((runtime_seconds / 3600))
    minutes=$(( (runtime_seconds % 3600) / 60 ))
    seconds=$((runtime_seconds % 60))
    echo "Decompression of gz files and compression into zstd for $file_path: $hours hours, $minutes minutes, $seconds seconds"
fi

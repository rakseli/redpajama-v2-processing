#!/bin/bash
#SBATCH --job-name=gzip_document
#SBATCH --output=../logs/gzip_document_all_%j.output # Name of stdout output file
#SBATCH --error=../logs/gzip_document_all_%j.error  # Name of stderr error file
#SBATCH --account=project_462000353
#SBATCH --time=24:00:00                           #
#SBATCH --ntasks=1                                # Number of tasks                     
#SBATCH --ntasks-per-node=1                       # Number of tasks per node
#SBATCH --cpus-per-task=84                        # N cpus
#SBATCH --mem-per-cpu=1000                        
#SBATCH --partition=small

module purge
module load LUMI/22.12 
module load parallel/20230322

srun \
        bash gzip_parallel.sh document
        

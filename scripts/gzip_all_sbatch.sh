#!/bin/bash
#SBATCH --job-name=gzip_document
#SBATCH --output=../logs/gzip_document_all_%j.output
#SBATCH --error=../logs/gzip_document_all_%j.error
#SBATCH --account=project_462000353
#SBATCH --time=24:00:00                           
#SBATCH --ntasks=1                                                   
#SBATCH --ntasks-per-node=1                       
#SBATCH --cpus-per-task=84                        
#SBATCH --mem-per-cpu=1000                        
#SBATCH --partition=small

module purge
module load LUMI/22.12 
module load parallel/20230322

srun \
        bash gzip_parallel.sh document
        

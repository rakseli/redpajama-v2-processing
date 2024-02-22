#!/bin/bash

run_time=$1

srun --account=project_462000444 --partition=small --time=02:00:00 --mem-per-cpu=2000 --cpus-per-task=2 --nodes=1 --pty bash
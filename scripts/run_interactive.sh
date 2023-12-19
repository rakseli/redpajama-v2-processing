#!/bin/bash

run_time=$1

srun --account=project_462000086 --partition=small --time="$run_time" --nodes=1 --pty bash
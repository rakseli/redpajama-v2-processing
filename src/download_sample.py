#!/usr/bin/env python3

import sys
import os

from datasets import load_dataset

script_dir = os.path.dirname(os.path.realpath(__file__))
cache_dir = os.path.join(script_dir, 'cache')

dataset = load_dataset(
    'togethercomputer/RedPajama-Data-V2',
    name='sample',
    cache_dir=cache_dir
)

for s, d in dataset.items():
    d.to_json(f'redpajama-v2-sample-{s}.jsonl')

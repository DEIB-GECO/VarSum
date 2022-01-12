#!/bin/bash

source /home/alfonsi/miniconda3/etc/profile.d/conda.sh
conda activate data_summarization
cd /home/alfonsi/data_summarization_1KGP
python main.py server geco geco78 5432 WARNING 

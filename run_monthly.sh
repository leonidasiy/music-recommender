#!/bin/bash

# Change to project directory
cd "/Users/lirenzhang/Desktop/Cursor CLI/music-recommender"

# Activate virtual environment
source venv/bin/activate

unalias python

# Run the pipeline
python src/main.py >> cron.log 2>&1

# Deactivate
deactivate

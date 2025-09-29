#!/bin/bash

export TABLE_NAME="motl6-participant-search-items"
export GOOGLE_API_KEY=$(cat ../.api_key)
export AWS_DEFAULT_REGION="us-east-1"

python search.py

## Overview

This repo consists of three components:
1. Update script that checks spreadsheets for updates and writes them to DynamoDB
2. Simple REST API to read participants and events (filtered by participant)
3. Static website that uses the API from #2 to enable searching by participant name


## Build

```sh
sam build && sam deploy --parameter-overrides="GoogleApiKey=$(cat .api_key)"
```

## Test

```sh
# Ad-hoc testing
pip install -r requirements.txt
export TABLE_NAME="{fill out}"
python src/api.py
python src/search.py

# API Gateway testing
sam local start-api
curl http://localhost:3000/participants
curl http://localhost:3000/events?participant=vyhd
```

import os
import json
import boto3
import traceback

from typing import TypedDict

class ProxyResponse(TypedDict):
    statusCode: int
    body: str

TABLE_CLIENT = boto3.client("dynamodb")
TABLE_NAME = os.environ["TABLE_NAME"]

PARTICIPANT_INDEX_NAME = "Participants"  # must agree with template.yaml GSI


def list_participants_lambda(event, context) -> ProxyResponse:
    try:
        args = dict(TableName=TABLE_NAME, IndexName=PARTICIPANT_INDEX_NAME)
        paginator = TABLE_CLIENT.get_paginator("scan").paginate(**args)

        # [{"Items": {"name": {"S": "vyhd"}, ...}, ...] --> ["vyhd", ...]
        names = [item["name"]["S"] for page in paginator for item in page["Items"]]
        response = {"names": sorted(names)}

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


def list_participant_events_lambda(event, context) -> ProxyResponse:
    try:
        participant_name = event["pathParameters"]["name"]

        args = dict(TableName=TABLE_NAME, Key={"name": participant_name})
        response = TABLE_CLIENT.get_item(**args)["Item"]["events"]

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except ValueError:
        return {"statusCode": 400, "body": "Participant name was not provided."}
    except TABLE_CLIENT.exceptions.ResourceNotFoundException:
        return {"statusCode": 404, "body": f"Participant '{participant_name} not found."}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


if __name__ == "__main__":
    # Do a quick li'l self test
    print(f"List participants: {list_participants_lambda({{}}, {{}})}")
    print(f"List events: {list_participant_events_lambda({"pathParameters": {"name": "vyhd"}}, {{}})}")

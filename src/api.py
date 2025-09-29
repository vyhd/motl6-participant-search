import os
import json
import boto3
import traceback

from typing import TypedDict

class ProxyResponse(TypedDict):
    statusCode: int
    body: str

TABLE_NAME = os.environ["TABLE_NAME"]
PARTICIPANT_INDEX_NAME = "Participants"  # must agree with template.yaml GSI

API_CLIENT = boto3.client("dynamodb")
TABLE_CLIENT = boto3.resource("dynamodb").Table(TABLE_NAME)


def list_participants_lambda(event, context) -> ProxyResponse:
    try:
        args = dict(TableName=TABLE_NAME, IndexName=PARTICIPANT_INDEX_NAME)
        paginator = API_CLIENT.get_paginator("scan").paginate(**args)

        # [{"Items": {"name": {"S": "vyhd"}, ...}, ...] --> ["vyhd", ...]
        # (plus a hack to filter out the metadata key ._.)
        names = [item["name"]["S"] for page in paginator for item in page["Items"]]
        names = [name for name in names if not name.startswith("__")]

        response = {"names": sorted(names)}

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


def list_participant_events_lambda(event, context) -> ProxyResponse:
    participant_name = event.get("pathParameters", {}).get("name")
    try:
        result = TABLE_CLIENT.get_item(Key={"name": participant_name})
        response = {
            "name": participant_name,
            "events": result["Item"]["events"],
        }

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except API_CLIENT.exceptions.ResourceNotFoundException:
        return {"statusCode": 404, "body": f"Participant '{participant_name} not found."}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


if __name__ == "__main__":
    # Do a quick li'l self test
    print("List participants...")
    list_participants_lambda({}, {})

    print("List participant events...")
    list_participant_events_lambda({"pathParameters": {"name": "vyhd-testing"}}, {})

import json
import traceback
from typing import Optional, TypedDict

class ProxyResponse(TypedDict):
    """The response structure expected by API Gateway."""
    statusCode: int
    headers: dict
    body: Optional[str]

def resp(status_code: int, body: Optional[str] = None) -> ProxyResponse:
    """Returns an HTTP response for this API, including CORS headers on every response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": body
    }

from .participants import ParticipantTable
_TABLE = ParticipantTable()

def list_participants_lambda(event, context) -> ProxyResponse:
    try:
        names = _TABLE.list_participants()

        # quick hack to strip out more names that shouldn't really be in there,
        # but are getting caught up an in edge case around comma + newline parses
        names = [n for n in names if not n.startswith("Day ") and not n.startswith("Pool ")]
        response = {"names": sorted(names, key=str.lower)}

        print(f"Response: {response}")
        return resp(200, json.dumps(response))
    except Exception as e:
        traceback.print_exc()
        return resp(500, repr(e))


def list_participant_events_lambda(event, context) -> ProxyResponse:
    participant_name = event.get("queryStringParameters", {}).get("name")

    try:
        events = _TABLE.list_events(participant_name)

        response = {
            "name": participant_name,
            "events": events,
        }

        print(f"Response: {response}")
        return resp(200, json.dumps(response))
    except _TABLE.ResourceNotFoundException:
        return resp(404, f"Participant '{participant_name} not found.")
    except Exception as e:
        traceback.print_exc()
        return resp(500, repr(e))


def get_last_update_time_lambda(event, context) -> ProxyResponse:
    try:
        metadata = _TABLE.get_metadata()

        if metadata:
            print(f"Response: {metadata}")
            return resp(200, json.dumps(metadata.get("lastUpdate", "never")))
        else:
            return resp(204)
    except Exception as e:
        traceback.print_exc()
        return resp(500, repr(e))


if __name__ == "__main__":
    # Do a quick li'l self test
    print("List participants...")
    list_participants_lambda({}, {})

    print("List participant events...")
    list_participant_events_lambda({"queryParameters": {"name": "vyhd-testing"}}, {})

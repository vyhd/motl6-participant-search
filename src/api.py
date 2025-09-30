import json
import traceback
from typing import Optional, TypedDict

class ProxyResponse(TypedDict):
    statusCode: int
    body: Optional[str]


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
        return {"statusCode": 200, "body": json.dumps(response)}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


def list_participant_events_lambda(event, context) -> ProxyResponse:
    participant_name = event.get("queryStringParameters", {}).get("name")

    try:
        events = _TABLE.list_events(participant_name)

        response = {
            "name": participant_name,
            "events": events,
        }

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except _TABLE.ResourceNotFoundException:
        return {"statusCode": 404, "body": f"Participant '{participant_name} not found."}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


def get_last_update_time_lambda(event, context) -> ProxyResponse:
    try:
        metadata = _TABLE.get_metadata()

        if metadata:
            print(f"Response: {metadata}")
            return {"statusCode": 200, "body": json.dumps(metadata.get("lastUpdate", "never"))}
        else:
            return {"statusCode": 204, "body": None}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


if __name__ == "__main__":
    # Do a quick li'l self test
    print("List participants...")
    list_participants_lambda({}, {})

    print("List participant events...")
    list_participant_events_lambda({"queryParameters": {"name": "vyhd-testing"}}, {})

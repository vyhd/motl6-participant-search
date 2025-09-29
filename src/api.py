import json
import traceback
from typing import TypedDict

class ProxyResponse(TypedDict):
    statusCode: int
    body: str


from .participants import ParticipantTable
_TABLE = ParticipantTable()

def list_participants_lambda(event, context) -> ProxyResponse:
    try:
        response = {"names": sorted(_TABLE.list_participants())}

        print(f"Response: {response}")
        return {"statusCode": 200, "body": json.dumps(response)}
    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": repr(e)}


def list_participant_events_lambda(event, context) -> ProxyResponse:
    participant_name = event.get("queryParameters", {}).get("name")
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


if __name__ == "__main__":
    # Do a quick li'l self test
    print("List participants...")
    list_participants_lambda({}, {})

    print("List participant events...")
    list_participant_events_lambda({"queryParameters": {"name": "vyhd-testing"}}, {})

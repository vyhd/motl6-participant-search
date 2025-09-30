from typing import Iterable, Literal, Optional, TypedDict

class Event(TypedDict):
  day: Literal["Thursday", "Friday", "Saturday", "Sunday"]
  category: str
  event: str
  time: str


class ParticipantTable:
    PARTICIPANT_INDEX_NAME = "Participants"  # must agree with template.yaml GSI
    METADATA_ITEM_KEY = "__meta__"

    def __init__(self):
        import os
        import boto3

        self.table_name = os.environ["TABLE_NAME"]

        self.api_client = boto3.client("dynamodb")
        self.table_client = boto3.resource("dynamodb").Table(self.table_name)

        # Make exceptions catchable from the outer context
        self.ResourceNotFoundException = self.api_client.exceptions.ResourceNotFoundException

    def get_metadata(self) -> Optional[dict]:
        """Returns the metadata value, or None if the key doesn't exist."""
        payload = self.table_client.get_item(Key={"name": self.METADATA_ITEM_KEY})
        return payload["Item"] if "Item" in payload else None

    def put_metadata(self, metadata: dict) -> None:
        """Replaces the metadata with new content."""
        metadata_item = {**metadata, **{"name": self.METADATA_ITEM_KEY}}
        self.table_client.put_item(Item=metadata_item)

    #
    # Public facing APIs
    #

    def list_participants(self) -> Iterable[str]:
        paginator = self.api_client.get_paginator("scan").paginate(
            TableName=self.table_name,
            IndexName=self.PARTICIPANT_INDEX_NAME
        )

        # [{"Items": {"name": {"S": "vyhd"}, ...}, ...] --> ["vyhd", ...]
        # (plus a hack to filter out the metadata key ._.)
        names = [item["name"]["S"] for page in paginator for item in page["Items"]]
        names = [name for name in names if not name.startswith("__")]

        return names

    def list_events(self, participant_name: str) -> Iterable[Event]:
        response = self.table_client.get_item(Key={"name": participant_name})
        return response["Item"]["events"]

    #
    # Update job APIs
    #

    def write_events(self, participant_events: dict[str, list[Event]]) -> None:
        """Writes events for each user."""
        with self.table_client.batch_writer() as batch:
            for name, events in participant_events.items():
                batch.put_item(Item={"name": name, "events": events})

    def delete_all_participants(self) -> None:
        """Lists, then removes, all keys in the table."""
        names = self.list_participants()

        with self.table_client.batch_writer() as batch:
            for name in names:
                batch.delete_item(Key={"name": name})

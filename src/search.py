#!/usr/bin/env python

import boto3
import gspread
import os
import re
from collections import defaultdict
from typing import Literal, Optional, TypedDict

class Event(TypedDict):
  day: Literal["Thursday", "Friday", "Saturday", "Sunday"]
  event: str
  time: str

TABLE_NAME = os.environ["TABLE_NAME"]
SHEETS_API_KEY = os.environ["GOOGLE_API_KEY"]

API_CLIENT = boto3.client("dynamodb")
TABLE_CLIENT = boto3.resource("dynamodb").Table(TABLE_NAME)

# this sheet is tied tightly to the traversal code below, so it may as well live here too
EVENT_SCHEDULE_SHEET_ID = "1vjNzS_-PXPbEyCr7LfQ2iaV58iwOHDRlXBFjq96Kyhw"

METADATA_ITEM_KEY = "__meta__"

class NotFoundException(Exception):
  pass


def delete_all_entries() -> None:
  """Lists, then removes, all keys in the table."""
  # shameless copy+paste, pending a refactor
  PARTICIPANT_INDEX_NAME = "Participants"  # must agree with template.yaml GSI
  args = dict(TableName=TABLE_NAME, IndexName=PARTICIPANT_INDEX_NAME)
  paginator = API_CLIENT.get_paginator("scan").paginate(**args)

  # [{"Items": {"name": {"S": "vyhd"}, ...}, ...] --> ["vyhd", ...]
  # (plus a hack to filter out the metadata key ._.)
  names = [item["name"]["S"] for page in paginator for item in page["Items"]]

  with TABLE_CLIENT.batch_writer() as batch:
    for name in names:
      batch.delete_item(Key={"name": name})

def sheet_needs_update(sheet: gspread.Spreadsheet) -> bool:
  """If True, the spreadsheet is updated and we need new records. If False, it can be skipped."""
  last_update = sheet.lastUpdateTime

  try:
    payload = TABLE_CLIENT.get_item(Key={"name": METADATA_ITEM_KEY})

    # we can get an empty payload with no item. sigh. if that happens, punt to the exception handler
    if not payload.get("Item"):
      raise NotFoundException()

    metadata = payload["Item"]
    last_seen_update = metadata["lastUpdate"][sheet.id]

    if last_update != last_seen_update:
      print(f"'{sheet.title}' updated at {last_update}, last processed at {last_seen_update}. Updating.")

      metadata["lastUpdate"][sheet.id] = last_update
      TABLE_CLIENT.put_item(Item=metadata)
      return True
  except (API_CLIENT.exceptions.ResourceNotFoundException, NotFoundException):
    print(f"'{sheet.title}' has never been processed. Updating.")

    TABLE_CLIENT.put_item(Item={
      "name": METADATA_ITEM_KEY,
      "lastUpdate": {
        sheet.id: last_update
      }
    })
    return True

  print(f"'{sheet.title}' is up to date ({last_update}), skipping.")
  return False


def update_from_event_schedule(gspread_client) -> None:
  sheet = gspread_client.open_by_key(EVENT_SCHEDULE_SHEET_ID)
  if not sheet_needs_update(sheet):
    return

  delete_all_entries()  # TODO: if we pull in volunteer data, factor this one out

  # Ideally, we'd be able to pull the formatting for these cells and flag 12-pt format cells
  # as a header, but I don't see a way to get it from gspread, so we hoof it with this regex.
  header_regex = re.compile(r"^(?:Classic|Singles|Doubles|Gauntlet|Team|Co-Op|Pool|Set|Top|Winner|Loser|Final|Last Chance|Callbacks|Gig|Seed|SF|WaNT|#|Extra Time|MAINT|GROUP)[\s\S]*$")

  def is_header(value: str) -> bool:
    matches = header_regex.match(value)
    print(f"is_header('{value}') -> {matches}")
    return matches is not None

  participant_events = defaultdict(list)

  for day in sheet.worksheets():
    # for my convenience, re-arrange this big ol' list into a 2D dict of (1-indexed) rows and cols.
    # this feels silly, but makes username->event resolution easier to think about in the next step.
    sheet_cells = defaultdict(defaultdict)
    for cell in day.get_all_cells():
      sheet_cells[cell.row][cell.col] = cell

    # skip headers, hold onto refs for the names we see
    for cols in sheet_cells.values():
      for cell in cols.values():
        if cell.row == 1 or cell.col == 1 or not cell.value or is_header(cell.value):
          continue

        # remove stray newlines and spaces from names. Newlines are often used in place
        # of commas when listing player names, but not always, so we fake them here and
        # remove the inevitable '' entry later on.
        names = [n.strip() for n in cell.value.replace("\n", ",").split(",")]

        event_tag = {
          "day": day.title,
          "event": sheet_cells[cell.row - 1][cell.col].value,
          "time": sheet_cells[cell.row][1].value,
        }

        [participant_events[n].append(event_tag) for n in names]

  del participant_events[""]

  with TABLE_CLIENT.batch_writer() as batch:
    for name, events in participant_events.items():
      batch.put_item(Item={"name": name, "events": events})


if __name__ == "__main__":
  try:
    gspread_client = gspread.api_key(SHEETS_API_KEY)
    update_from_event_schedule(gspread_client)
  except Exception as e:
    import traceback
    traceback.print_exc()

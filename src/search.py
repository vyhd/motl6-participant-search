#!/usr/bin/env python

import boto3
import gspread
import os
import re
from collections import defaultdict
from typing import TypedDict, Literal


class Event(TypedDict):
  day: Literal["Thursday", "Friday", "Saturday", "Sunday"]
  event: str
  time: str


API_KEY = os.environ["GOOGLE_API_KEY"]
EVENT_SCHEDULE_SHEET_ID = "1vjNzS_-PXPbEyCr7LfQ2iaV58iwOHDRlXBFjq96Kyhw"


def event_schedule_by_participants(gspread_client) -> dict[str, list[Event]]:
  # Ideally, we'd be able to pull the formatting for these cells and flag 12-pt format cells
  # as a header, but I don't see a way to get it from gspread, so we hoof it with this regex.
  header_regex = re.compile(r"^(?:Classic|Singles|Doubles|Gauntlet|Team|Co-Op|Pool|Top|Winner|Loser|Final|Last Chance|Callbacks|Gig|Seed|SF|WaNT|#|MAINT|GROUP).*$")
  def is_header(value: str) -> bool:
    return header_regex.match(value) is not None

  sheet = gspread_client.open_by_key(EVENT_SCHEDULE_SHEET_ID)
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
  return participant_events


if __name__ == "__main__":
  try:
    gspread_client = gspread.api_key(API_KEY)
    participant_events = event_schedule_by_participants(gspread_client)
  except Exception as e:
    breakpoint()
    print("ded")

  breakpoint()

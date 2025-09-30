#!/usr/bin/env python

import gspread
import os
import re
from collections import defaultdict

SHEETS_API_KEY = os.environ["GOOGLE_API_KEY"]

from .participants import ParticipantTable, Event
_TABLE = ParticipantTable()

# this sheet is tied tightly to the traversal code below, so it may as well live here too
EVENT_SCHEDULE_SHEET_ID = "1tAGWcnSkPZmMhpDqkyeGkSeAvICGGRCBpXlqkh6GVJU"


def sheet_needs_update(sheet: gspread.Spreadsheet) -> bool:
  """If True, the spreadsheet is updated and we need new records. If False, it can be skipped."""
  last_update = sheet.lastUpdateTime
  metadata = _TABLE.get_metadata()
  last_seen_update = metadata and metadata["lastUpdate"].get(sheet.id) or "never"

  if not metadata or last_update != last_seen_update:
    print(f"'{sheet.title}' updated at {last_update}, we last updated {last_seen_update}. Updating.")
    return True
  else:
    print(f"'{sheet.title}' is up to date ({last_update}), skipping.")
    return False


def update_timestamp(sheet: gspread.Spreadsheet) -> None:
  metadata = _TABLE.get_metadata()

  if metadata:
    metadata["lastUpdate"][sheet.id] = sheet.lastUpdateTime
  else:
    metadata = {"lastUpdate": {sheet.id: sheet.lastUpdateTime}}

  _TABLE.put_metadata(metadata)


def update_from_event_schedule(gspread_client) -> None:
  sheet = gspread_client.open_by_key(EVENT_SCHEDULE_SHEET_ID)

  if not sheet_needs_update(sheet):
    return

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

        event_tag: Event = {
          "category": sheet_cells[1][cell.col].value,  # e.g. "DDR White (1)"
          "day": day.title,
          "event": sheet_cells[cell.row - 1][cell.col].value, # e.g. "WaNT Acc Pool A (11-13)"
          "time": sheet_cells[cell.row - 1][1].value,  # e.g. "11:30 AM" - must align with title, not name
        }

        [participant_events[n].append(event_tag) for n in names]

  del participant_events[""]

  _TABLE.write_events(participant_events)
  update_timestamp(sheet)


def handler(event, context):
  try:
    gspread_client = gspread.api_key(SHEETS_API_KEY)
    update_from_event_schedule(gspread_client)
  except Exception as e:
    import traceback
    traceback.print_exc()

#!/usr/bin/env python
from abc import ABC, abstractmethod
from functools import cached_property

import gspread
import os
import re
from datetime import datetime, date, timedelta
from collections import defaultdict

SHEETS_API_KEY = os.environ["GOOGLE_API_KEY"]

from .participants import ParticipantTable, Event
PARTICIPANTS_TABLE = ParticipantTable()

GSPREAD_CLIENT = gspread.api_key(SHEETS_API_KEY)


class Spreadsheet(ABC):
  """Encapsulates a spreadsheet. We subclass this for different sheet structures."""
  def __init__(self, sheet_id: str):
    self.id = sheet_id

  @cached_property
  def sheet(self) -> gspread.Spreadsheet:
    return GSPREAD_CLIENT.open_by_key(self.id)

  def needs_update(self) -> bool:
    if metadata := PARTICIPANTS_TABLE.get_metadata():
      if metadata["lastUpdate"].get(self.id) == self.sheet.lastUpdateTime:
        return False

    print(f"'{self.sheet.title}' updated at {self.sheet.lastUpdateTime}, needs update.")
    return True

  def update_timestamp(self) -> None:
    """Sets the metadata timestamp to the spreadsheet's latest update time."""
    metadata = PARTICIPANTS_TABLE.get_metadata() or {"lastUpdate": {}}
    metadata["lastUpdate"][self.id] = self.sheet.lastUpdateTime
    PARTICIPANTS_TABLE.put_metadata(metadata)

  @abstractmethod
  def update(self) -> dict[str, list[Event]]:
    """Returns a list of Events, keyed by username, from this sheet."""
    pass


class EventSpreadsheet(Spreadsheet):
  def __init__(self):
    super().__init__(sheet_id="1tAGWcnSkPZmMhpDqkyeGkSeAvICGGRCBpXlqkh6GVJU")

  def update(self) -> dict[str, list[Event]]:
    # Ideally, we'd be able to pull the formatting for these cells and flag 12-pt format cells
    # as a header, but I don't see a way to get it from gspread, so we hoof it with this regex.
    header_regex = re.compile(r"^(?:Classic|Singles|Doubles|Gauntlet|Team|Co-Op|Pool|Set|Top|Winner|Loser|Final|Last Chance|Callbacks|Gig|Seed|SF|WaNT|#|Extra Time|MAINT|GROUP)[\s\S]*$")

    def is_header(value: str) -> bool:
      matches = header_regex.match(value)
      return matches is not None

    player_events = defaultdict(list)

    for day_sheet in self.sheet.worksheets():
      # for my convenience, re-arrange this big ol' list into a 2D dict of (1-indexed) rows and cols.
      # this feels silly, but makes username->event resolution easier to think about in the next step.
      sheet_cells = defaultdict(defaultdict)
      for cell in day_sheet.get_all_cells():
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
            "day": day_sheet.title,
            "event": sheet_cells[cell.row - 1][cell.col].value,  # e.g. "WaNT Acc Pool A (11-13)"
            "time": sheet_cells[cell.row - 1][1].value,  # e.g. "11:30 AM" - must align with title, not name
          }

          [player_events[n].append(event_tag) for n in names]

    del player_events[""]
    return player_events


class VolunteerSpreadsheet(Spreadsheet):
  def __init__(self):
    super().__init__(sheet_id="1xuiaO5AuWNCPoDmYjgjzH7escQ339T03DSggPbeYOQc")

  def update(self) -> dict[str, list[Event]]:
    # big ol' hack: some participant names differ in this spreadsheet, but we want to match them
    # to the event schedule list (which is case-sensitive).
    user_lookup_table = {
      "Ambones": "ambones",
      "Androopy": "ANDROO",
      "BishYama": "Yama",
      "CeilingFam": "CELNGFAM",
      "Ceilingfam": "CELNGFAM",
      "DDR-kelvin": "DDR KELVIN",
      "ddrkeby": "DDR KEBY",
      "ddrdjwaffle": "DJ Waffle",
      "Devi": "DEVI",
      "driodx": "Driodx",
      "dylan yono": "yono",
      "emcat": "EMCAT",
      "Grandbassist1209": "GrandBassist",
      "italianfalchion": "ItalianFalchion",
      "its_gdon": "GDon",
      "kaberosi": "Kaberosi",
      "Kidcrab": "kidcrab",
      "larksford": "Larks",
      "Kimalaka": "kimalaka",
      "Nemo": "nemo",
      "nightmanflock": "Nightman",
      "pball": "Pat Ball",
      "sbubby": "SBUBBY",
      "Sora": "SO~RA",
      "Sveta": "sveta",
      "theregoesmysanity": "ThereGoesMySanity",

      # not sure about these ones:
      # "Mustang": "STANGIEX",
      # "Sirius2point0": "SIRIUS",
      # "whiskr_": "whiskr",
    }

    # sometimes we see "foo - setup" and sometimes we see "setup - foo", so handle both cases
    setup_regex = re.compile(r"^[Ss]etup - (.*)$|^(.*) - [Ss]etup$")
    volunteer_events = defaultdict(list)

    for day_sheet in self.sheet.worksheets():
      # this sheet is labeled "Thursday Schedule", "Friday Schedule", etc - lop off the end bit
      sheet_cells = defaultdict(defaultdict)
      for cell in day_sheet.get_all_cells():
        sheet_cells[cell.row][cell.col] = cell

      for cols in sheet_cells.values():
        for cell in cols.values():
          # ignore the headers when we're enumerating users
          if cell.row < 3 or cell.col == 1 or not cell.value:
            continue

          # peek at the cell above us - if its contents are identical, we're a continuation of
          # an already-processed event and can be skipped.
          if sheet_cells[cell.row - 1][cell.col].value == cell.value:
            continue

          # categories are in large merged cells, but gspread only returns the value of the
          # leftmost cell. walk leftward from our column until we find the right value
          category = next(sheet_cells[1][c].value for c in range(cell.col, 0, -1) if sheet_cells[1][c].value)
          day = day_sheet.title.split(" ")[0]  # e.g., "Thursday Schedule" -> "Thursday"
          event = sheet_cells[2][cell.col].value  # e.g., "Head TO"
          time = sheet_cells[cell.row][1].value  # e.g., "11:00 AM"

          if match := setup_regex.match(cell.value):
            name = match.group(1) or match.group(2)
            event = f"{event} (setup)"
          else:
            name = cell.value

          # resolve the name, to ensure these get intermingled with player events where appropriate
          name = user_lookup_table[name] if name in user_lookup_table else name

          volunteer_events[name].append({
            "category": category,
            "day": day,
            "event": event,
            "time": time
          })

    return volunteer_events


def event_order(event: Event) -> datetime:
  """Parses an `event` into a number matching its place in time. This lets us sort events "naturally"."""
  try:
    days_of_event = ["Thursday", "Friday", "Saturday", "Sunday"]
    print(f"event: {event}")
    day_index = days_of_event.index(event["day"])

    # parse the time, then pin the date to our starting day plus the index above
    event_dt = datetime.strptime(event["time"], "%I:%M %p")
    event_dt = event_dt.replace(year=2025, month=10, day=2) + timedelta(days=day_index)
    return event_dt
  except Exception as e:
    import pdb
    pdb.pm()

def handler(event, context):
  try:
    event_sheet = EventSpreadsheet()
    volunteer_sheet = VolunteerSpreadsheet()

    if event_sheet.needs_update() or volunteer_sheet.needs_update():
      player_events = event_sheet.update()
      volunteer_events = volunteer_sheet.update()

      # TODO: intelligently merge both dicts, incl. sorting events
      all_events = defaultdict(list)

      all_names = {*player_events.keys(), *volunteer_events.keys()}
      for name in all_names:
        all_events[name] = sorted([*player_events.get(name, []), *volunteer_events.get(name, [])], key=event_order)

      PARTICIPANTS_TABLE.delete_all_participants()
      PARTICIPANTS_TABLE.write_events(all_events)

      event_sheet.update_timestamp()
      volunteer_sheet.update_timestamp()
  except Exception as e:
    import traceback
    traceback.print_exc()

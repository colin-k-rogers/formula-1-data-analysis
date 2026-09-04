# Agent notes

- Keep this repo and the deployed MotherDuck Flights in sync. Every Flight under
  `flights/<name>/` (`main.py`, `requirements.txt`) should match what's actually
  deployed via `create_flight`/`update_flight`/`edit_flight_source`. When you
  change one, change the other in the same session: edit the deployed Flight
  and commit the matching file(s) here, or vice versa. Don't let the repo copy
  drift into documentation-only status.
- Keep code comments as short as they can be while still earning their place.
  Comment the non-obvious: why a value is what it is, a workaround and what
  forced it, a constraint the next reader would otherwise trip over. Don't
  restate what the code already says.
- Anything explained in `README.md` — setup, config keys, how the pieces fit
  together — lives there only. Point at it from the code instead of repeating
  it, so the two can't drift apart.

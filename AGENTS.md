# Agent notes

- Flights (`flights/<name>/`) and Dives (`dives/<name>/`) are [MotherDuck
  Blueprint](https://github.com/motherduckdb/motherduck-blueprints) packages,
  each with a `blueprint.yml` next to its source (`main.py`/`requirements.txt`
  for a Flight, `src/dive.tsx` for a Dive). Merging a change under `flights/**`
  or `dives/**` to `main` deploys it automatically via
  [.github/workflows/deploy_blueprints.yaml](.github/workflows/deploy_blueprints.yaml)
  — see the README's "Automated deploys" section.
- Because deploys are one-directional (git → MotherDuck), don't edit a
  deployed Flight or Dive directly (`update_flight`/`edit_flight_source`,
  `MD_UPDATE_DIVE_CONTENT`, the MotherDuck UI, etc.) without also committing
  the matching change here in the same session — a direct edit that isn't
  committed will get silently overwritten by the next merge to `main` that
  touches that package. If you ever do need to inspect what's actually live
  (e.g. to check for drift), use `get_flight`/`read_dive` and reconcile any
  difference into the repo file rather than assuming the repo copy is
  current.
- [.github/workflows/blueprints_doctor.yaml](.github/workflows/blueprints_doctor.yaml)'s
  `drift-check` job plans every Flight/Dive against production weekly,
  regardless of what changed in git, and publishes it to that run's job
  summary — a Flight/Dive edited directly in MotherDuck shows up there as a
  pending change even if nothing has touched its files in git recently. It's
  a spot-check surfaced for a human to read, not an automatic pass/fail gate,
  so still don't rely on it instead of following the rule above.
- A Dive's `export const REQUIRED_DATABASES = …` must stay on a single line:
  the deployer strips that declaration with a single-line regex, so a wrapped
  one deploys a Dive whose leftover array body is a syntax error. `make test`
  and the `Tests` workflow catch it on every PR — run `make test` after
  touching anything under `dives/**`.
- Keep code comments as short as they can be while still earning their place.
  Comment the non-obvious: why a value is what it is, a workaround and what
  forced it, a constraint the next reader would otherwise trip over. Don't
  restate what the code already says.
- Anything explained in `README.md` — setup, config keys, how the pieces fit
  together — lives there only. Point at it from the code instead of repeating
  it, so the two can't drift apart.

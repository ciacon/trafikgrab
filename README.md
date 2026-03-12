# trafikgrab

`trafikgrab` is an asyncio daemon for polling remote transit data, storing immutable UTC archives, and enforcing outage/budget controls.

The scheduler model is intentionally strict:
- exactly one real polling job (`poller`) uses an interval trigger
- cron jobs only reschedule that one poller interval ("gearbox" pattern)
- outage and quota controls pause/resume the poller instead of creating backlog floods

## Features

- APScheduler 3.x + `AsyncIOScheduler` + persistent SQLite job store
- single in-flight poll execution (`max_instances=1` + async lock)
- local-time schedule policy in `Europe/Stockholm`
- UTC/Zulu persistence for timestamps and archive naming
- conditional HTTP requests (`If-None-Match`, `If-Modified-Since`)
- immutable archived payloads with metadata sidecars for replay/audit
- monthly API budget enforcement with automatic monthly reset
- circuit breaker with pause + health-probe resume
- structured JSON logs via `loguru`
- single-instance lock file for process safety

## Requirements

- Python `>=3.11,<4.0`
- Poetry

## Installation

```bash
poetry install
```

## Quick Start

1. Create a config file from the example:

```bash
cp config.example.toml config.toml
```

2. Update at least:
- `base_url`
- `region`
- output/archive paths if needed

3. Run in foreground:

```bash
poetry run trafikgrab-daemon --config config.toml
```

You can also run:

```bash
poetry run python -m daemon.main --config config.toml
```

## Configuration

Configuration is loaded in this order:
1. defaults in code
2. TOML file (`[trafikgrab]` table)
3. environment overrides `TRAFIKGRAB_<KEY>`

Main keys (see [`config.example.toml`](config.example.toml)):

- `base_url`: remote endpoint to poll
- `output_path`: latest payload path (atomically replaced)
- `archive_root`: root for immutable archived snapshots
- `region`: token used in archive filename (`trafikgrab_{region}.pb`)
- `scheduler_timezone`: local policy timezone (default `Europe/Stockholm`)
- `scheduler_db_path`: APScheduler persistent DB
- `state_db_path`: daemon business-state DB
- `normal_interval_seconds`, `night_interval_seconds`, `peak_interval_seconds`
- `peak_am_start`, `peak_am_end`, `peak_pm_start`, `peak_pm_end`
- `monthly_limit`, `api_cost_per_poll`
- `breaker_failure_threshold`, `breaker_cooldown_seconds`, `breaker_probe_interval_seconds`
- `poller_misfire_grace_seconds`
- `lock_file`

## Scheduler Behavior

Default mode transitions:
- `00:00` -> night interval
- `06:00` -> normal interval
- `peak_am_start` -> peak interval
- `peak_am_end` -> normal interval
- `peak_pm_start` -> peak interval
- `peak_pm_end` -> normal interval

Monthly reset:
- day `1` at `00:00` local time resets monthly usage and resumes poller

Important invariants implemented:
- poll frequency never faster than every 2 seconds
- only one poller job definition is used (rescheduled, never duplicated)
- no overlapping poll executions
- paused poller does not accumulate work

## Archive and Replay Model

On each successful `200` response, trafikgrab writes:

1. Immutable payload archive:

```text
YYYY/MM/YYYYMMDD-HHMMSSZ_trafikgrab_{region}.pb
```

Example:

```text
2026/03/20260306-124501Z_trafikgrab_stockholm.pb
```

2. Sidecar metadata JSON next to payload (`.json`) containing replay/audit data such as:
- `downloaded_at_utc`
- `source_url`
- `region`
- `http_status`
- `etag`
- `last_modified`
- `sha256`
- `archive_path`

3. Latest snapshot file at `output_path` (overwritten atomically)

If two downloads occur in the same second, a numeric suffix is added to avoid collisions.

## Persistent State

Business state is stored in SQLite and includes:
- quota counters (`month_key`, `month_used`, `month_limit`)
- breaker state (`consecutive_failures`, `breaker_open_until_utc`)
- request/result metadata (`last_attempt_utc`, `last_success_utc`, status, etag/last-modified)
- archive pointers (`last_archive_path`, `last_archive_metadata_path`, checksum)

## Outage and Quota Handling

- Outage-like failures: network errors and HTTP 5xx
- Non-outage failures: HTTP 4xx (default behavior)
- After threshold outage failures, breaker opens and poller is paused
- Recovery job probes endpoint and resumes poller when healthy and cooldown has elapsed
- If monthly budget would be exceeded, poller is paused until monthly reset job runs

## Development

Run all required local checks:

```bash
tox -e check
```

Run checks individually:

```bash
tox -e lint
tox -e type
tox -e tests
tox -e py314
```

Direct type-check:

```bash
poetry run mypy src
```

## systemd

A service template is provided at [`systemd/trafikgrab.service`](systemd/trafikgrab.service).

Typical install flow:
- copy project to `/opt/trafikgrab`
- create venv and install dependencies
- place config at `/etc/trafikgrab/config.toml`
- adjust paths in unit file as needed
- enable and start service

## License

GNU GPL v2. See [`LICENSE`](LICENSE).

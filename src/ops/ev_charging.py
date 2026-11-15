"""Record the status and price history of public EV charging in the Netherlands.

SOURCE. NDW, the Dutch national access point for charging data, publishes every public charge
point with its live status and tariffs as open files that are refreshed continuously:

    https://opendata.ndw.nu/charging_point_locations_ocpi.json.gz   (~18 MB, OCPI 2.2.1 locations)
    https://opendata.ndw.nu/charging_point_tariffs_ocpi.json.gz     (~4.5 MB, OCPI tariffs)

NDW open data is CC0 1.0 unless a dataset says otherwise (docs.ndw.nu/algemeen/
data-gebruiksvoorwaarden, read 2026-11-15; nothing dataset-specific is stated for these files),
commercial reuse included. NDW states that it keeps NO history for this stream ("Er is geen
historische database ingericht voor deze datastroom"), so what is not recorded is gone.

WHAT IS STORED. Full snapshots every few minutes would be ~5 GB a day. Each EVSE carries its own
`last_updated`, so the recorder keeps state in memory and writes only CHANGES:

    e36_nl_evse_status_events   one row per observed change of (status, last_updated) of an EVSE
    e36_nl_evse_polls           one row per poll: counts per status, file timestamp, timings
    e36_nl_charge_points        once a day: every connector with its location, operator, power,
                                tariff ids, and the status at that moment (the day's baseline)
    e36_nl_tariffs              once a day: every tariff with its price components (JSON text)
    e36_nl_tariff_changes       tariffs that are new or changed inside a window

A window has no memory of the one before it. Its first poll therefore writes "catch-up" rows
(first_poll = true) for every EVSE whose status changed in the last CATCHUP_H hours, which
recovers the latest transition per charge point across the gap between two windows (gaps longer
than that are covered by the daily snapshot, which holds every status). Transitions
that were overwritten inside a gap, or inside one poll interval, are not recoverable; the polls
table shows exactly when the recorder was watching.

SEPARATE FROM THE PEER-TO-PEER COLLECTOR ON PURPOSE. Parsing the file holds the interpreter for
seconds, which would smear the arrival timestamps that collector exists to measure. This runs as
its own workflow and writes straight to the private Hugging Face archive.

usage: python -m src.ops.ev_charging [--duration S] [--poll S] [--archive]
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("DF_EV_DATA", ROOT / "data"))
BASE = "https://opendata.ndw.nu/"
LOCATIONS = BASE + "charging_point_locations_ocpi.json.gz"
TARIFFS = BASE + "charging_point_tariffs_ocpi.json.gz"
UA = "dataforge-ev-history (research archive; contact via huggingface.co/dataforge-labs)"
OWNER = os.environ.get("HF_OWNER", "dataforge-labs")
ARCHIVE_REPO = f"{OWNER}/dataforge-ephemeral"
DATASETS = ("e36_nl_evse_status_events", "e36_nl_evse_polls", "e36_nl_charge_points",
            "e36_nl_tariffs", "e36_nl_tariff_changes")
STATUSES = ("AVAILABLE", "CHARGING", "BLOCKED", "RESERVED", "OUTOFORDER", "INOPERATIVE",
            "UNKNOWN", "PLANNED", "REMOVED")
CATCHUP_H = 3.0
TARIFF_EVERY_S = 3600.0
CHECKPOINT_EVERY_S = 1800.0


def _ts(value):
    """OCPI timestamp text -> aware datetime, or None. Never raises: one bad value in 200,000
    must not cost a poll."""
    if not value:
        return None
    try:
        s = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None


def fetch(url: str, session: requests.Session, etag: str | None = None):
    """-> (payload or None when unchanged, etag, last_modified text, bytes). Raises on failure."""
    headers = {"User-Agent": UA}
    if etag:
        headers["If-None-Match"] = etag
    r = session.get(url, headers=headers, timeout=120)
    if r.status_code == 304:
        return None, etag, r.headers.get("Last-Modified"), 0
    r.raise_for_status()
    raw = r.content
    try:
        text = gzip.decompress(raw)
    except OSError:
        text = raw                       # the server may already have decoded it
    return json.loads(text), r.headers.get("ETag"), r.headers.get("Last-Modified"), len(raw)


def evse_state(locations) -> dict:
    """(location id, evse uid) -> (status, last_updated text). The uid alone is not unique."""
    state = {}
    for loc in locations:
        lid = loc.get("id")
        for e in loc.get("evses") or []:
            state[(lid, e.get("uid"))] = (e.get("status"), e.get("last_updated"))
    return state


def diff_events(prev: dict, cur: dict, poll_ts: datetime, first_poll: bool) -> list[tuple]:
    """Rows carry the location id ONLY for the ~1% of EVSE uids that occur under more than one
    location; for the rest the uid identifies the charge point and the daily snapshot gives its
    location. Two 36-character ids on every row would double the size of the table."""
    seen, dup = set(), set()
    for _, uid in list(cur) + [k for k in prev if k not in cur]:
        (dup if uid in seen else seen).add(uid)

    def loc(lid, uid):
        return lid if uid in dup else None

    rows = []
    if first_poll:
        floor = poll_ts.timestamp() - CATCHUP_H * 3600
        for (lid, uid), (status, upd) in cur.items():
            t = _ts(upd)
            if t is not None and t.timestamp() >= floor:
                rows.append((poll_ts, loc(lid, uid), uid, status, None, t, True))
        return rows
    for key, (status, upd) in cur.items():
        old = prev.get(key)
        if old is None or old[0] != status or old[1] != upd:
            rows.append((poll_ts, loc(*key), key[1], status, old[0] if old else None, _ts(upd), False))
    for key, old in prev.items():
        if key not in cur:               # dropped from the file: recorded, not guessed at
            rows.append((poll_ts, loc(*key), key[1], "__ABSENT__", old[0], None, False))
    return rows


EVENT_SCHEMA = pa.schema([
    ("run_id", pa.string()), ("poll_ts", pa.timestamp("ms", tz="UTC")),
    ("location_id", pa.string()), ("evse_uid", pa.string()), ("status", pa.string()),
    ("prev_status", pa.string()), ("status_updated", pa.timestamp("ms", tz="UTC")),
    ("first_poll", pa.bool_())])


def events_table(rows, run_id) -> pa.Table:
    cols = list(zip(*rows)) if rows else [[] for _ in range(7)]
    return pa.table({
        "run_id": pa.array([run_id] * len(rows), pa.string()),
        "poll_ts": pa.array(cols[0], pa.timestamp("ms", tz="UTC")),
        "location_id": pa.array(cols[1], pa.string()), "evse_uid": pa.array(cols[2], pa.string()),
        "status": pa.array(cols[3], pa.string()), "prev_status": pa.array(cols[4], pa.string()),
        "status_updated": pa.array(cols[5], pa.timestamp("ms", tz="UTC")),
        "first_poll": pa.array(cols[6], pa.bool_())}, schema=EVENT_SCHEMA)


def charge_points_frame(locations, poll_ts) -> pd.DataFrame:
    rows = []
    for loc in locations:
        co = loc.get("coordinates") or {}
        base = (loc.get("id"), loc.get("name"), loc.get("address"), loc.get("postal_code"),
                loc.get("city"), loc.get("country"), co.get("latitude"), co.get("longitude"),
                (loc.get("operator") or {}).get("name"), (loc.get("owner") or {}).get("name"),
                (loc.get("suboperator") or {}).get("name"), loc.get("party_id"),
                loc.get("parking_type"), loc.get("publish"), loc.get("last_updated"))
        for e in loc.get("evses") or []:
            ev = (e.get("uid"), e.get("evse_id"), e.get("status"), e.get("last_updated"),
                  "|".join(e.get("capabilities") or []) or None, e.get("physical_reference"))
            conns = e.get("connectors") or [{}]
            for c in conns:
                rows.append(base + ev + (
                    c.get("standard"), c.get("format"), c.get("power_type"),
                    c.get("max_voltage"), c.get("max_amperage"), c.get("max_electric_power"),
                    "|".join(c.get("tariff_ids") or []) or None, c.get("last_updated")))
    cols = ["location_id", "location_name", "address", "postal_code", "city", "country", "latitude",
            "longitude", "operator", "owner", "suboperator", "party_id", "parking_type", "publish",
            "location_updated", "evse_uid", "evse_id", "status", "status_updated", "capabilities",
            "physical_reference", "standard", "format", "power_type",
            "max_voltage", "max_amperage", "max_electric_power", "tariff_ids", "connector_updated"]
    df = pd.DataFrame(rows, columns=cols)
    for c in ("latitude", "longitude", "max_voltage", "max_amperage", "max_electric_power"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.insert(0, "snapshot_ts", poll_ts)
    return df


def tariff_key(t) -> tuple:
    return (t.get("party_id"), t.get("id"))


def tariffs_frame(tariffs, poll_ts) -> pd.DataFrame:
    rows = []
    for t in tariffs:
        mn, mx = t.get("min_price") or {}, t.get("max_price") or {}
        rows.append((t.get("country_code"), t.get("party_id"), t.get("id"), t.get("currency"),
                     t.get("type"), json.dumps(t.get("elements"), separators=(",", ":")),
                     mn.get("excl_vat") if isinstance(mn, dict) else None,
                     mx.get("excl_vat") if isinstance(mx, dict) else None,
                     t.get("start_date_time"), t.get("end_date_time"), t.get("last_updated")))
    df = pd.DataFrame(rows, columns=["country_code", "party_id", "tariff_id", "currency", "type",
                                     "elements_json", "min_price_excl_vat", "max_price_excl_vat",
                                     "start_date_time", "end_date_time", "tariff_updated"])
    for c in ("min_price_excl_vat", "max_price_excl_vat"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.insert(0, "snapshot_ts", poll_ts)
    return df


def _path(dataset: str, run_id: str) -> Path:
    d = DATA / dataset / run_id[:4] / run_id[5:7]
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{run_id}.parquet"


def write(dataset: str, table, run_id: str) -> None:
    """Atomic replace, so a checkpoint is always a complete file."""
    if isinstance(table, pd.DataFrame):
        if not len(table):
            return
        if "run_id" not in table.columns:
            table.insert(0, "run_id", run_id)
        table = pa.Table.from_pandas(table, preserve_index=False)
    if table.num_rows == 0:
        return
    p = _path(dataset, run_id)
    tmp = p.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, p)


def daily_snapshot_due(run_id: str) -> bool:
    """True when the archive has no charge-point snapshot for this UTC day yet. Any doubt means
    yes: a duplicate daily snapshot costs megabytes, a missing one cannot be made up."""
    if os.environ.get("DF_EV_SNAPSHOT") in ("0", "1"):
        return os.environ["DF_EV_SNAPSHOT"] == "1"
    token = os.environ.get("HF_TOKEN")
    if not token:
        return True
    try:
        from huggingface_hub import HfApi
        folder = f"e36_nl_charge_points/{run_id[:4]}/{run_id[5:7]}"
        names = [f.path.rsplit("/", 1)[-1] for f in
                 HfApi(token=token).list_repo_tree(ARCHIVE_REPO, path_in_repo=folder,
                                                   repo_type="dataset")]
        return not any(n.startswith(run_id[:10]) for n in names)
    except Exception as exc:  # noqa: BLE001 -- a missing folder is the normal first-day case
        print(f"  snapshot check: {type(exc).__name__}, taking the snapshot", flush=True)
        return True


def record(duration_s: float, poll_s: float) -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y-%m-%dT%H%MZ")
    session = requests.Session()
    snapshot = daily_snapshot_due(run_id)
    print(f"EV charging recorder {run_id}: {duration_s:.0f}s, poll every {poll_s:.0f}s, "
          f"daily snapshot {'yes' if snapshot else 'already archived'}", flush=True)
    state: dict = {}
    events: list[tuple] = []
    polls: list[dict] = []
    tariff_seen: dict = {}
    tariff_changes: list[pd.DataFrame] = []
    etag = None
    first = True
    last_tariff = 0.0
    last_checkpoint = time.monotonic()
    deadline = time.monotonic() + duration_s
    failures = 0

    def checkpoint():
        write("e36_nl_evse_status_events", events_table(events, run_id), run_id)
        write("e36_nl_evse_polls", pd.DataFrame(polls), run_id)
        if tariff_changes:
            write("e36_nl_tariff_changes", pd.concat(tariff_changes, ignore_index=True), run_id)

    while True:
        t0 = time.monotonic()
        poll_ts = datetime.now(timezone.utc).replace(microsecond=0)
        row = {"poll_ts": poll_ts, "ok": False, "unchanged": False, "error": None}
        try:
            payload, etag, last_mod, nbytes = fetch(LOCATIONS, session, etag)
            row.update(file_last_modified=last_mod, bytes=nbytes,
                       fetch_s=round(time.monotonic() - t0, 2))
            if payload is None:
                row.update(ok=True, unchanged=True)
            else:
                cur = evse_state(payload)
                new = diff_events(state, cur, poll_ts, first)
                events.extend(new)
                counts = {}
                for status, _ in cur.values():
                    counts[status] = counts.get(status, 0) + 1
                row.update(ok=True, locations=len(payload), evses=len(cur), events=len(new),
                           **{f"n_{s.lower()}": counts.get(s, 0) for s in STATUSES})
                if first and snapshot:
                    write("e36_nl_charge_points", charge_points_frame(payload, poll_ts), run_id)
                state, first = cur, False
                del payload
            failures = 0
        except Exception as exc:  # noqa: BLE001 -- one failed poll must not end the window
            failures += 1
            row["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
            etag = None
        row["total_s"] = round(time.monotonic() - t0, 2)
        polls.append(row)
        if len(polls) % 6 == 1 or row["error"]:
            print(f"  {poll_ts:%H:%M:%S} evses {row.get('evses')} charging {row.get('n_charging')} "
                  f"events {row.get('events')} total {len(events):,} "
                  f"{'ERROR ' + row['error'] if row['error'] else ''}", flush=True)

        if time.monotonic() - last_tariff >= TARIFF_EVERY_S or not tariff_seen:
            last_tariff = time.monotonic()
            try:
                tariffs, _, _, _ = fetch(TARIFFS, session)
                cur_t = {tariff_key(t): t.get("last_updated") for t in tariffs}
                if not tariff_seen:
                    if snapshot:
                        write("e36_nl_tariffs", tariffs_frame(tariffs, poll_ts), run_id)
                else:
                    changed = [t for t in tariffs if tariff_seen.get(tariff_key(t), "") != t.get("last_updated")]
                    if changed:
                        tariff_changes.append(tariffs_frame(changed, poll_ts))
                    print(f"  tariffs: {len(tariffs):,} listed, {len(changed):,} new or changed", flush=True)
                tariff_seen = cur_t
                del tariffs
            except Exception as exc:  # noqa: BLE001
                print(f"  tariffs: {type(exc).__name__}: {str(exc)[:120]}", flush=True)

        if time.monotonic() - last_checkpoint >= CHECKPOINT_EVERY_S:
            last_checkpoint = time.monotonic()
            checkpoint()
        if failures >= 12:
            print("  !! twelve consecutive failed polls, stopping this window", flush=True)
            break
        wait = poll_s - (time.monotonic() - t0)
        if time.monotonic() + max(wait, 0) >= deadline:
            break
        if wait > 0:
            time.sleep(wait)

    checkpoint()
    good = sum(1 for p in polls if p["ok"])
    print(f"  done: {good} of {len(polls)} polls ok, {len(events):,} status events, "
          f"{sum(len(t) for t in tariff_changes):,} tariff changes", flush=True)
    return 0 if good else 1


def archive() -> int:
    """Additive upload of the e36 tables to the private archive. Exits 0 on failure by design:
    the workflow keeps the files as a build artifact, so a failed upload loses nothing."""
    from huggingface_hub import HfApi
    token = os.environ.get("HF_TOKEN")
    files = [p for d in DATASETS for p in (DATA / d).rglob("*.parquet")] if DATA.exists() else []
    if not files:
        print("  nothing to archive", flush=True)
        return 0
    if not token:
        print("  !! HF_TOKEN not set, not archived", flush=True)
        return 0
    for attempt in range(4):
        try:
            HfApi(token=token).upload_folder(
                repo_id=ARCHIVE_REPO, repo_type="dataset", folder_path=str(DATA),
                allow_patterns=[f"{d}/**/*.parquet" for d in DATASETS],
                commit_message=f"e36 EV charging: {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z")
            size = sum(p.stat().st_size for p in files)
            print(f"  archived {len(files)} files, {size / 1e6:.1f} MB, to {ARCHIVE_REPO}", flush=True)
            (DATA / ".e36_archived").write_text("ok", encoding="utf-8")
            return 0
        except Exception as exc:  # noqa: BLE001
            print(f"  !! upload attempt {attempt + 1} failed: {type(exc).__name__}: {str(exc)[:140]}", flush=True)
            time.sleep(30 * (attempt + 1))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=float(os.environ.get("DF_EV_DURATION_S", 16200)))
    ap.add_argument("--poll", type=float, default=float(os.environ.get("DF_EV_POLL_S", 300)))
    ap.add_argument("--archive", action="store_true")
    a = ap.parse_args()
    return archive() if a.archive else record(a.duration, a.poll)


if __name__ == "__main__":
    sys.exit(main())

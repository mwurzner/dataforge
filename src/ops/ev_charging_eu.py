"""Record the status and price history of public EV charging in Finland and France.

The same idea as src/ops/ev_charging.py (the Netherlands), for two more national access points
under the EU charging-data rules (AFIR). Neither source keeps history, so what is not recorded is
gone. Both licences allow commercial reuse WITH ATTRIBUTION; any product built on these tables
must credit the source:

  FINLAND  Fintraffic Digitraffic AFIR API, https://afir.digitraffic.fi/api/charging-network/v1/
           Licence: Creative Commons 4.0 BY, "even commercially, as long as you credit the source"
           (https://www.digitraffic.fi/en/terms-of-service/, read 2026-11-16).
           Credit line: "Source: Fintraffic / digitraffic.fi, licence CC 4.0 BY".
           ~20,200 charging points; status of every EVSE in one file (statuses/all, ~350 kB);
           locations ~24 MB; tariffs with VAT rates. Requests send "Digitraffic-User" and gzip.

  FRANCE   Base nationale consolidée IRVE, transport.data.gouv.fr (dataset 685965d0ab901919e14a4a1b)
           Licence: Licence Ouverte / Open Licence 2.0 (Etalab), commercial reuse allowed with
           attribution. Credit line: "Source: transport.data.gouv.fr, Base nationale consolidée
           IRVE, Licence Ouverte 2.0".
           Dynamic file (resource 84098, ~9.5 MB CSV, refreshed within minutes): state and
           occupancy of ~118,000 charge points (pdc), one or more rows per pdc. Static file
           (resource 84013, ~130 MB CSV): every pdc with station, operator, power and a free-text
           tariff field. Status covers ~65% of French pdc; some timestamps are months old.

WHAT IS STORED (same pattern as the Dutch e36 tables: changes only, plus a daily baseline)

    e37_fi_evse_status_events   one row per observed change of (status, lastUpdatedAt) of an EVSE
    e37_fi_polls                one row per poll: counts per status, timings
    e37_fi_charge_points        once a day: every connector with location, operator, power, tariffs
    e37_fi_tariffs              once a day: every tariff, price elements as JSON text
    e37_fi_tariff_changes       tariffs new or changed inside a window (checked hourly)
    e37_fi_operators            once a day: the operator list
    e38_fr_pdc_status_events    one row per observed change of (etat, occupation, horodatage) of a pdc
    e38_fr_polls                one row per poll: counts per state and occupancy, timings
    e38_fr_static               once a day: the whole static file, every column as text, without
                                the operators' contact e-mails and phone numbers

As in the Dutch recorder, the first poll of a window writes catch-up rows (first_poll = true) for
everything whose own timestamp lies within CATCHUP_H hours, which recovers the latest change per
charge point across the gap between two windows. Each country fails independently: one source
being down never stops the other.

usage: python -m src.ops.ev_charging_eu [--duration S] [--poll S] [--archive]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from src.ops.ev_charging import ARCHIVE_REPO, CATCHUP_H, CHECKPOINT_EVERY_S, DATA, UA, _ts, write

FI_BASE = "https://afir.digitraffic.fi/api/charging-network/v1/"
FI_STATUSES = FI_BASE + "locations/statuses?limit=ALL"
FI_LOCATIONS = FI_BASE + "locations?limit=ALL"
FI_TARIFFS = FI_BASE + "tariffs?limit=ALL"
FI_OPERATORS = FI_BASE + "operators"
FR_DYNAMIC = "https://transport.data.gouv.fr/resources/84098/download"
FR_STATIC = "https://transport.data.gouv.fr/resources/84013/download"
HEADERS = {"User-Agent": UA, "Digitraffic-User": "DataForge", "Accept-Encoding": "gzip"}
DATASETS = ("e37_fi_evse_status_events", "e37_fi_polls", "e37_fi_charge_points", "e37_fi_tariffs",
            "e37_fi_tariff_changes", "e37_fi_operators", "e38_fr_pdc_status_events", "e38_fr_polls",
            "e38_fr_static")
FI_STATUSES_KNOWN = ("AVAILABLE", "CHARGING", "BLOCKED", "RESERVED", "OUTOFORDER", "INOPERATIVE",
                     "UNKNOWN", "PLANNED", "REMOVED")
FR_PRISES = ("etat_prise_type_2", "etat_prise_type_combo_ccs", "etat_prise_type_chademo",
             "etat_prise_type_ef")
TARIFF_EVERY_S = 3600.0
MAX_FAILURES = 12


def get(url: str, session: requests.Session, timeout: int = 120) -> requests.Response:
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r


def _ts_list(values):
    return pd.to_datetime(pd.Series([_ts(v) for v in values], dtype="object"), utc=True)


# ---------------------------------------------------------------- Finland

def fi_state(payload) -> dict:
    """evseId -> (status, lastUpdatedAt text)."""
    return {s.get("evseId"): (s.get("status"), s.get("lastUpdatedAt"))
            for s in payload.get("statuses") or [] if s.get("evseId")}


def fi_diff(prev: dict, cur: dict, poll_ts: datetime, first: bool) -> list[tuple]:
    rows = []
    if first:
        floor = poll_ts.timestamp() - CATCHUP_H * 3600
        for k, (status, upd) in cur.items():
            t = _ts(upd)
            if t is not None and t.timestamp() >= floor:
                rows.append((poll_ts, k, status, None, upd, True))
        return rows
    for k, (status, upd) in cur.items():
        old = prev.get(k)
        if old is None or old != (status, upd):
            rows.append((poll_ts, k, status, old[0] if old else None, upd, False))
    for k, old in prev.items():
        if k not in cur:
            rows.append((poll_ts, k, "__ABSENT__", old[0], None, False))
    return rows


def fi_events_frame(rows, run_id) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["poll_ts", "evse_id", "status", "prev_status", "status_updated",
                                     "first_poll"])
    df["status_updated"] = _ts_list(df["status_updated"])
    df["poll_ts"] = pd.to_datetime(df["poll_ts"], utc=True)
    df.insert(0, "run_id", run_id)
    return df


def fi_charge_points_frame(payload, poll_ts) -> pd.DataFrame:
    rows = []
    for f in payload.get("features") or []:
        p = f.get("properties") or {}
        coords = (f.get("geometry") or {}).get("coordinates") or [None, None]
        base = (p.get("id"), p.get("name"), json.dumps(p.get("operator"), ensure_ascii=False),
                json.dumps(p.get("address"), ensure_ascii=False), coords[1], coords[0],
                p.get("chargingWhenClosed"), json.dumps(p.get("openingTimes"), ensure_ascii=False),
                p.get("modifiedAt"))
        for e in p.get("evses") or [{}]:
            ev = (e.get("id"), "|".join(e.get("capabilities") or []) or None, e.get("physicalReference"),
                  e.get("floorLevel"), e.get("modifiedAt"))
            for c in e.get("connectors") or [{}]:
                rows.append(base + ev + (c.get("standard"), c.get("format"), c.get("powerType"),
                                         c.get("maxVoltage"), c.get("maxAmperage"),
                                         c.get("maxElectricPower"),
                                         "|".join(c.get("tariffIds") or []) or None, c.get("modifiedAt")))
    df = pd.DataFrame(rows, columns=[
        "location_id", "location_name", "operator_json", "address_json", "latitude", "longitude",
        "charging_when_closed", "opening_times_json", "location_modified", "evse_id", "capabilities",
        "physical_reference", "floor_level", "evse_modified", "standard", "format", "power_type",
        "max_voltage", "max_amperage", "max_electric_power", "tariff_ids", "connector_modified"])
    for c in ("latitude", "longitude", "max_voltage", "max_amperage", "max_electric_power"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["charging_when_closed"] = df["charging_when_closed"].astype("boolean")
    df.insert(0, "snapshot_ts", poll_ts)
    return df


def fi_tariff_key(t) -> tuple:
    return (t.get("partyId"), t.get("id"))


def fi_tariffs_frame(tariffs, poll_ts) -> pd.DataFrame:
    df = pd.DataFrame([(t.get("countryCode"), t.get("partyId"), t.get("id"), t.get("currency"),
                        t.get("type"), t.get("taxIncluded"),
                        json.dumps(t.get("elements"), separators=(",", ":")), t.get("lastUpdated"))
                       for t in tariffs],
                      columns=["country_code", "party_id", "tariff_id", "currency", "type",
                               "tax_included", "elements_json", "tariff_updated"])
    df.insert(0, "snapshot_ts", poll_ts)
    return df


def fi_tariff_list(payload) -> list:
    return payload.get("tariffs") if isinstance(payload, dict) else (payload or [])


# ---------------------------------------------------------------- France

def fr_state(text: str) -> tuple[dict, int]:
    """id_pdc_itinerance -> (etat, occupation, horodatage, prises...), keeping the newest row when a
    pdc appears more than once (several sources feed the consolidation). -> (state, raw rows)."""
    state, n = {}, 0
    for r in csv.DictReader(io.StringIO(text)):
        n += 1
        k = r.get("id_pdc_itinerance")
        if not k:
            continue
        v = (r.get("etat_pdc") or None, r.get("occupation_pdc") or None, r.get("horodatage") or "") + \
            tuple(r.get(p) or None for p in FR_PRISES)
        old = state.get(k)
        if old is None or v[2] > old[2]:
            state[k] = v
    return state, n


def fr_diff(prev: dict, cur: dict, poll_ts: datetime, first: bool) -> list[tuple]:
    rows = []
    if first:
        floor = poll_ts.timestamp() - CATCHUP_H * 3600
        for k, v in cur.items():
            t = _ts(v[2])
            if t is not None and t.timestamp() >= floor:
                rows.append((poll_ts, k) + v + (None, None, True))
        return rows
    for k, v in cur.items():
        old = prev.get(k)
        if old is None or old != v:
            rows.append((poll_ts, k) + v + ((old[0], old[1]) if old else (None, None)) + (False,))
    for k, old in prev.items():
        if k not in cur:
            rows.append((poll_ts, k, "__ABSENT__", None, None) + (None,) * len(FR_PRISES) +
                        (old[0], old[1], False))
    return rows


def fr_events_frame(rows, run_id) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["poll_ts", "id_pdc_itinerance", "etat_pdc", "occupation_pdc",
                                     "horodatage", *FR_PRISES, "prev_etat_pdc", "prev_occupation_pdc",
                                     "first_poll"])
    df["horodatage"] = _ts_list(df["horodatage"])
    df["poll_ts"] = pd.to_datetime(df["poll_ts"], utc=True)
    df.insert(0, "run_id", run_id)
    return df


FR_DROP = ("contact_amenageur", "contact_operateur", "telephone_operateur")


def fr_static_frame(text: str, poll_ts) -> pd.DataFrame:
    """Every column as text, minus the contact e-mails and phone numbers: some operators are
    private persons, and nothing we would sell needs a way to reach them."""
    df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
    df = df.drop(columns=[c for c in FR_DROP if c in df.columns])
    df.insert(0, "snapshot_ts", poll_ts)
    return df


def polls_frame(polls) -> pd.DataFrame:
    """Fixed text type for the error column, so every window's file has the same schema."""
    df = pd.DataFrame(polls)
    if "error" in df.columns:
        df["error"] = df["error"].astype("string")
    return df


# ---------------------------------------------------------------- shared

def daily_snapshot_due(dataset: str, run_id: str) -> bool:
    """True when the archive has no file of `dataset` for this UTC day yet. Any doubt means yes."""
    if os.environ.get("DF_EV_SNAPSHOT") in ("0", "1"):
        return os.environ["DF_EV_SNAPSHOT"] == "1"
    token = os.environ.get("HF_TOKEN")
    if not token:
        return True
    try:
        from huggingface_hub import HfApi
        names = [f.path.rsplit("/", 1)[-1] for f in HfApi(token=token).list_repo_tree(
            ARCHIVE_REPO, path_in_repo=f"{dataset}/{run_id[:4]}/{run_id[5:7]}", repo_type="dataset")]
        return not any(n.startswith(run_id[:10]) for n in names)
    except Exception as exc:  # noqa: BLE001 -- a missing folder is the normal first-day case
        print(f"  snapshot check {dataset}: {type(exc).__name__}, taking the snapshot", flush=True)
        return True


def record(duration_s: float, poll_s: float) -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y-%m-%dT%H%MZ")
    session = requests.Session()
    snap_fi = daily_snapshot_due("e37_fi_charge_points", run_id)
    snap_fr = daily_snapshot_due("e38_fr_static", run_id)
    print(f"EV charging recorder FI+FR {run_id}: {duration_s:.0f}s, poll every {poll_s:.0f}s, "
          f"daily snapshot FI {'yes' if snap_fi else 'done'}, FR {'yes' if snap_fr else 'done'}", flush=True)

    fi = {"state": {}, "events": [], "polls": [], "first": True, "failures": 0}
    fr = {"state": {}, "events": [], "polls": [], "first": True, "failures": 0}
    tariff_seen: dict = {}
    tariff_changes: list[pd.DataFrame] = []
    last_tariff = 0.0
    last_checkpoint = time.monotonic()
    deadline = time.monotonic() + duration_s

    def checkpoint():
        write("e37_fi_evse_status_events", fi_events_frame(fi["events"], run_id), run_id)
        write("e37_fi_polls", polls_frame(fi["polls"]), run_id)
        write("e38_fr_pdc_status_events", fr_events_frame(fr["events"], run_id), run_id)
        write("e38_fr_polls", polls_frame(fr["polls"]), run_id)
        if tariff_changes:
            write("e37_fi_tariff_changes", pd.concat(tariff_changes, ignore_index=True), run_id)

    def poll_fi(poll_ts):
        t0 = time.monotonic()
        row = {"poll_ts": poll_ts, "ok": False, "error": None}
        try:
            r = get(FI_STATUSES, session)
            payload = r.json()
            cur = fi_state(payload)
            new = fi_diff(fi["state"], cur, poll_ts, fi["first"])
            fi["events"].extend(new)
            counts: dict = {}
            for status, _ in cur.values():
                counts[status] = counts.get(status, 0) + 1
            row.update(ok=True, evses=len(cur), events=len(new), bytes=len(r.content),
                       source_modified=payload.get("modifiedAt"),
                       **{f"n_{s.lower()}": counts.get(s, 0) for s in FI_STATUSES_KNOWN})
            if fi["first"] and snap_fi:
                write("e37_fi_charge_points", fi_charge_points_frame(get(FI_LOCATIONS, session).json(), poll_ts), run_id)
                ops = get(FI_OPERATORS, session).json()
                ops = ops.get("operators", ops) if isinstance(ops, dict) else ops
                write("e37_fi_operators", pd.DataFrame(
                    [{"snapshot_ts": poll_ts, "operator_json": json.dumps(o, ensure_ascii=False)} for o in ops]), run_id)
            fi["state"], fi["first"], fi["failures"] = cur, False, 0
        except Exception as exc:  # noqa: BLE001 -- one failed poll must not end the window
            fi["failures"] += 1
            row["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        row["total_s"] = round(time.monotonic() - t0, 2)
        fi["polls"].append(row)
        return row

    def poll_fr(poll_ts):
        t0 = time.monotonic()
        row = {"poll_ts": poll_ts, "ok": False, "error": None}
        try:
            r = get(FR_DYNAMIC, session)
            cur, nrows = fr_state(r.content.decode("utf-8-sig"))
            new = fr_diff(fr["state"], cur, poll_ts, fr["first"])
            fr["events"].extend(new)
            etat: dict = {}
            occ: dict = {}
            for v in cur.values():
                etat[v[0]] = etat.get(v[0], 0) + 1
                occ[v[1]] = occ.get(v[1], 0) + 1
            row.update(ok=True, rows=nrows, pdc=len(cur), events=len(new), bytes=len(r.content),
                       newest=max((v[2] for v in cur.values()), default=None),
                       n_en_service=etat.get("en_service", 0), n_hors_service=etat.get("hors_service", 0),
                       n_etat_inconnu=etat.get("inconnu", 0), n_libre=occ.get("libre", 0),
                       n_occupe=occ.get("occupe", 0), n_reserve=occ.get("reserve", 0),
                       n_occupation_inconnu=occ.get("inconnu", 0))
            if fr["first"] and snap_fr:
                write("e38_fr_static", fr_static_frame(get(FR_STATIC, session, 300).content.decode("utf-8-sig"), poll_ts), run_id)
            fr["state"], fr["first"], fr["failures"] = cur, False, 0
        except Exception as exc:  # noqa: BLE001
            fr["failures"] += 1
            row["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        row["total_s"] = round(time.monotonic() - t0, 2)
        fr["polls"].append(row)
        return row

    while True:
        t0 = time.monotonic()
        poll_ts = datetime.now(timezone.utc).replace(microsecond=0)
        a, b = poll_fi(poll_ts), poll_fr(poll_ts)
        if len(fi["polls"]) % 6 == 1 or a["error"] or b["error"]:
            print(f"  {poll_ts:%H:%M:%S} FI evses {a.get('evses')} events {a.get('events')} "
                  f"{'ERROR ' + a['error'] if a['error'] else ''}| FR pdc {b.get('pdc')} events {b.get('events')} "
                  f"{'ERROR ' + b['error'] if b['error'] else ''}", flush=True)

        if time.monotonic() - last_tariff >= TARIFF_EVERY_S or not tariff_seen:
            last_tariff = time.monotonic()
            try:
                tariffs = fi_tariff_list(get(FI_TARIFFS, session).json())
                cur_t = {fi_tariff_key(t): t.get("lastUpdated") for t in tariffs}
                if not tariff_seen:
                    if snap_fi:
                        write("e37_fi_tariffs", fi_tariffs_frame(tariffs, poll_ts), run_id)
                else:
                    changed = [t for t in tariffs if tariff_seen.get(fi_tariff_key(t), "") != t.get("lastUpdated")]
                    if changed:
                        tariff_changes.append(fi_tariffs_frame(changed, poll_ts))
                tariff_seen = cur_t
            except Exception as exc:  # noqa: BLE001
                print(f"  FI tariffs: {type(exc).__name__}: {str(exc)[:120]}", flush=True)

        if time.monotonic() - last_checkpoint >= CHECKPOINT_EVERY_S:
            last_checkpoint = time.monotonic()
            checkpoint()
        if fi["failures"] >= MAX_FAILURES and fr["failures"] >= MAX_FAILURES:
            print("  !! both sources failed twelve polls in a row, stopping this window", flush=True)
            break
        wait = poll_s - (time.monotonic() - t0)
        if time.monotonic() + max(wait, 0) >= deadline:
            break
        if wait > 0:
            time.sleep(wait)

    checkpoint()
    ok_fi = sum(1 for p in fi["polls"] if p["ok"])
    ok_fr = sum(1 for p in fr["polls"] if p["ok"])
    print(f"  done: FI {ok_fi}/{len(fi['polls'])} polls ok, {len(fi['events']):,} events; "
          f"FR {ok_fr}/{len(fr['polls'])} polls ok, {len(fr['events']):,} events; "
          f"{sum(len(t) for t in tariff_changes):,} FI tariff changes", flush=True)
    return 0 if (ok_fi or ok_fr) else 1


def archive() -> int:
    """Additive upload of the e37/e38 tables to the private archive. Exits 0 on failure by design:
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
                commit_message=f"e37/e38 EV charging FI+FR: {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z")
            size = sum(p.stat().st_size for p in files)
            print(f"  archived {len(files)} files, {size / 1e6:.1f} MB, to {ARCHIVE_REPO}", flush=True)
            (DATA / ".e37_archived").write_text("ok", encoding="utf-8")
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

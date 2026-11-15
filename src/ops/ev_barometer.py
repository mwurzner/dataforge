"""Laadbarometer: a public snapshot page of Dutch public charging, built from NDW's open files.

Each run takes one snapshot of every public charge point (status and published price), adds it
to a rolling history of snapshots, and publishes a small JSON file plus a static page as a
Hugging Face Space. Nothing here uses the recorder's private archive: the page is made from the
same CC0 source files, read at the moment of the run.

WHAT IS COUNTED, AND WHY THE WORDING IS CAREFUL. The statuses are the operators' own reports.
"Storing" is OCPI status OUTOFORDER only. INOPERATIVE (not yet in service, or switched off) and
UNKNOWN (no usable status delivered) are shown separately rather than counted as faults, because
an operator that reports nothing would otherwise look better than one that reports honestly.
REMOVED and PLANNED charge points are left out altogether.

PRICES are the ENERGY component of the published ad hoc tariff, which OCPI defines as excluding
VAT; they are shown including 21% VAT. Start fees and time fees are not included. A connector
with day/night elements gets the median of its energy prices.

usage: python -m src.ops.ev_barometer [--out DIR] [--publish]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.ops.ev_charging import LOCATIONS, TARIFFS, fetch  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site" / "laadbarometer"
OWNER = os.environ.get("HF_OWNER", "dataforge-labs")
SPACE = f"{OWNER}/laadbarometer"
VAT = 1.21
MIN_OPERATOR_POINTS = 1000
MIN_CITY_POINTS = 300
HISTORY_DAYS = 14
AVERAGE_DAYS = 7
MIN_SNAPSHOTS_FOR_AVERAGE = 12
COUNTED = ("AVAILABLE", "CHARGING", "OUTOFORDER", "INOPERATIVE", "UNKNOWN")


def energy_price(tariff) -> float | None:
    """Price per kWh excluding VAT, or None when the tariff has no usable energy price."""
    free, restricted = [], []
    for el in tariff.get("elements") or []:
        for pc in el.get("price_components") or []:
            if pc.get("type") != "ENERGY" or pc.get("price") is None:
                continue
            try:
                p = float(pc["price"])
            except (TypeError, ValueError):
                continue
            if 0.05 <= p <= 2.0:                  # zero and absurd values are data errors
                (restricted if el.get("restrictions") else free).append(p)
    if free:
        return statistics.median(free)
    if restricted:
        return statistics.median(restricted)
    return None


def _quantiles(values):
    if not values:
        return None
    v = sorted(values)
    q = lambda x: v[min(len(v) - 1, int(x * (len(v) - 1) + 0.5))]  # noqa: E731
    return {"n": len(v), "median": round(q(0.5) * VAT, 3), "p10": round(q(0.1) * VAT, 3),
            "p90": round(q(0.9) * VAT, 3)}


def snapshot(locations, tariffs, now: datetime, file_time: str | None) -> dict:
    by_party, by_id = {}, {}
    for t in tariffs:
        p = energy_price(t)
        if p is not None:
            by_party[(t.get("party_id"), t.get("id"))] = p
            by_id.setdefault(t.get("id"), p)
    ops = defaultdict(Counter)
    op_prices = defaultdict(list)
    cities = defaultdict(list)
    city_names = defaultdict(Counter)
    nat = Counter()
    ac, dc = [], []
    for loc in locations:
        op = ((loc.get("operator") or {}).get("name") or loc.get("party_id") or "onbekend").strip()
        city = " ".join((loc.get("city") or "").split())
        for e in loc.get("evses") or []:
            status = e.get("status")
            if status in ("REMOVED", "PLANNED"):
                continue
            key = status if status in COUNTED else "OTHER"
            nat[key] += 1
            nat["points"] += 1
            ops[op][key] += 1
            ops[op]["points"] += 1
            price, fast = None, False
            for c in e.get("connectors") or []:
                for tid in c.get("tariff_ids") or []:
                    price = by_party.get((loc.get("party_id"), tid), by_id.get(tid))
                    if price is not None:
                        break
                if price is not None:
                    fast = c.get("power_type") == "DC"
                    break
            if price is None:
                continue
            (dc if fast else ac).append(price)
            if not fast:
                op_prices[op].append(price)
                if city:
                    cities[city.casefold()].append(price)
                    city_names[city.casefold()][city] += 1

    def counts(c):
        return {k.lower(): c.get(k, 0) for k in COUNTED} | {"points": c["points"], "other": c.get("OTHER", 0)}

    operators = []
    for name, c in ops.items():
        if c["points"] >= MIN_OPERATOR_POINTS:
            row = {"name": name} | counts(c)
            q = _quantiles(op_prices.get(name))
            # A price from a small minority of an operator's charge points would mislead.
            row["price_ac"] = q["median"] if q and q["n"] >= 100 and q["n"] >= 0.25 * c["points"] else None
            row["price_ac_n"] = q["n"] if q else 0
            operators.append(row)
    operators.sort(key=lambda r: -r["points"])
    city_rows = [{"city": city_names[k].most_common(1)[0][0], "n": len(v),
                  "median": round(statistics.median(v) * VAT, 3)}
                 for k, v in cities.items() if len(v) >= MIN_CITY_POINTS]
    city_rows.sort(key=lambda r: r["median"])
    return {"generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "source_file_time": file_time,
            "national": counts(nat), "operators": operators,
            "prices": {"ac": _quantiles(ac), "dc": _quantiles(dc)},
            "cities": {"cheapest": city_rows[:10], "dearest": city_rows[-10:][::-1],
                       "counted": len(city_rows)}}


def history_entry(snap: dict) -> dict:
    keys = ("points", "outoforder", "inoperative", "unknown", "charging")
    return {"ts": snap["generated"], "nat": [snap["national"][k] for k in keys],
            "ops": {o["name"]: [o[k] for k in keys] for o in snap["operators"]}}


def averages(history: list[dict], now: datetime) -> dict | None:
    """Mean share per operator over the last AVERAGE_DAYS, from the snapshots collected so far."""
    floor = (now - timedelta(days=AVERAGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    recent = [h for h in history if h["ts"] >= floor]
    if len(recent) < MIN_SNAPSHOTS_FOR_AVERAGE:
        return None

    def mean_share(rows, i):
        shares = [r[i] / r[0] for r in rows if r and r[0]]
        return round(100 * sum(shares) / len(shares), 2) if shares else None

    out = {"snapshots": len(recent), "from": recent[0]["ts"], "to": recent[-1]["ts"],
           "national": {"outoforder": mean_share([h["nat"] for h in recent], 1),
                        "unknown": mean_share([h["nat"] for h in recent], 3),
                        "charging": mean_share([h["nat"] for h in recent], 4)},
           "operators": {}}
    names = {n for h in recent for n in h["ops"]}
    for n in names:
        rows = [h["ops"][n] for h in recent if n in h["ops"]]
        if len(rows) >= MIN_SNAPSHOTS_FOR_AVERAGE:
            out["operators"][n] = {"outoforder": mean_share(rows, 1), "inoperative": mean_share(rows, 2),
                                   "unknown": mean_share(rows, 3), "charging": mean_share(rows, 4),
                                   "snapshots": len(rows)}
    return out


def load_history(token: str | None) -> list[dict]:
    """The Space is the only place the history lives. A missing file is the normal first run."""
    try:
        from huggingface_hub import hf_hub_download
        p = hf_hub_download(SPACE, "history.json", repo_type="space", token=token, force_download=True)
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"  no earlier history ({type(exc).__name__}); starting a new one", flush=True)
        return []


def build(out: Path, token: str | None) -> dict:
    session = requests.Session()
    now = datetime.now(timezone.utc)
    locations, _, file_time, _ = fetch(LOCATIONS, session)
    tariffs, _, _, _ = fetch(TARIFFS, session)
    snap = snapshot(locations, tariffs, now, file_time)
    history = load_history(token)
    floor = (now - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    history = [h for h in history if h.get("ts", "") >= floor] + [history_entry(snap)]
    snap["average"] = averages(history, now)
    out.mkdir(parents=True, exist_ok=True)
    for f in SITE.iterdir():
        if f.is_file():
            shutil.copy(f, out / f.name)
    (out / "latest.json").write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    (out / "history.json").write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
    n = snap["national"]
    print(f"  snapshot {snap['generated']}: {n['points']:,} charge points, "
          f"{100 * n['outoforder'] / n['points']:.1f}% out of order, {100 * n['unknown'] / n['points']:.1f}% unknown, "
          f"{len(snap['operators'])} operators listed, {len(history)} snapshots in history", flush=True)
    return snap


def publish(out: Path, token: str) -> None:
    from huggingface_hub import HfApi
    api = HfApi(token=token)
    api.create_repo(repo_id=SPACE, repo_type="space", space_sdk="static", private=False, exist_ok=True)
    api.upload_folder(repo_id=SPACE, repo_type="space", folder_path=str(out),
                      commit_message=f"snapshot {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z")
    print(f"  published to https://huggingface.co/spaces/{SPACE}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "dist" / "laadbarometer"))
    ap.add_argument("--publish", action="store_true")
    a = ap.parse_args()
    token = os.environ.get("HF_TOKEN")
    out = Path(a.out)
    build(out, token)
    if a.publish:
        if not token:
            print("  !! HF_TOKEN not set, not published", flush=True)
            return 1
        publish(out, token)
    return 0


if __name__ == "__main__":
    sys.exit(main())

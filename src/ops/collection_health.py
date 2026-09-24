"""Small provider-level quality summaries; measurements and error rows stay intact."""
import json
import os

import pandas as pd


def summarize(tables):
    rows = []
    for dataset, frames in tables.items():
        frames = [f for f in list(frames) if f is not None and len(f)]
        if not frames:
            rows.append(dict(dataset=dataset, provider='all', rows=0, error_rows=0,
                             status='no_observations', last_received_ts=None))
            continue
        df = pd.concat(frames, ignore_index=True)
        grouping = 'provider' if 'provider' in df else ('venue' if 'venue' in df else None)
        for provider, group in df.groupby(grouping, dropna=False) if grouping else [('all', df)]:
            errors = int(group.error.notna().sum()) if 'error' in group else 0
            times = next((pd.to_numeric(group[c], errors='coerce') for c in
                          ['response_received_ts','sampled_ts','quoted_ts','round_ts'] if c in group), None)
            last = float(times.max()) if times is not None and times.notna().any() else None
            rows.append(dict(dataset=dataset, provider=str(provider), rows=len(group),
                             error_rows=errors, status='degraded' if errors else 'observed',
                             last_received_ts=last))
    return rows


def report(tables, failures):
    rows = summarize(tables)
    bad = [r for r in rows if r['status'] != 'observed']
    print('Feed health: ' + json.dumps(rows), flush=True)
    if bad:
        failures.append('feed health: '+', '.join(f"{r['dataset']}/{r['provider']} {r['error_rows']}/{r['rows']} errors ({r['status']})" for r in bad)[:400])
        print('::warning::Feed-level errors or missing panels; inspect collection health summary', flush=True)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary,'a',encoding='utf-8') as handle:
            handle.write('\n### Collection health\n\n| Table | Provider | Rows | Errors | Status |\n|---|---|---:|---:|---|\n')
            for r in rows:
                handle.write(f"| {r['dataset']} | {r['provider']} | {r['rows']} | {r['error_rows']} | {r['status']} |\n")
    return rows

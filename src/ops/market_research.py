"""Collect a compact hourly research sample; optional additive, budget-checked private upload."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.ephemeral.market_research import collect

REPO = 'dataforge-labs/dataforge-ephemeral'
DATASET = 'e32_market_research'
DAY_BUDGET = 5_000_000
TOTAL_BUDGET = 2_000_000_000
ARCHIVE_GUARD = 90_000_000_000
FILE_BUDGET = 256_000
SCHEMA = pa.schema([pa.field(k,t) for k,t in [
    ('schema_version',pa.int32()),('run_id',pa.string()),('cycle_id',pa.string()),
    ('source',pa.string()),('kind',pa.string()),('key',pa.string()),
    ('request_started_ts',pa.float64()),('response_received_ts',pa.float64()),
    ('latency_s',pa.float64()),('http_status',pa.int32()),('error',pa.string()),
    ('source_url',pa.string()),('payload_json',pa.string())]])


def check_budget(info, path, size):
    if not info.private:
        raise ValueError('Research observations must stay private')
    if size > FILE_BUDGET:
        raise ValueError('Research snapshot exceeds 256 KB limit; retained in workflow artifact only')
    day_prefix = path.rsplit('/',1)[0]+'/'
    files = [f for f in info.siblings if f.rfilename.startswith(DATASET+'/')]
    if any(f.size is None for f in info.siblings):
        raise ValueError('Incomplete file-size inventory; refusing an unbudgeted upload')
    today = sum(f.size for f in files if f.rfilename.startswith(day_prefix))
    total = sum(f.size for f in files)
    archive = max(sum(f.size for f in info.siblings), getattr(info,'used_storage',None) or 0)
    if today+size > DAY_BUDGET or total+size > TOTAL_BUDGET or archive+size > ARCHIVE_GUARD:
        raise ValueError('Research storage limit reached; existing observations retained, new upload stopped')
    return dict(day_before_bytes=today, research_before_bytes=total, archive_before_bytes=archive)


def publish(api, local, remote):
    from huggingface_hub import CommitOperationAdd
    data = local.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    info = api.dataset_info(REPO, files_metadata=True)
    budget = check_budget(info,remote,len(data))
    if any(f.rfilename == remote for f in info.siblings):
        raise ValueError('An observation already exists at this path; refusing to overwrite it')
    result = api.create_commit(REPO,repo_type='dataset',parent_commit=info.sha,
        operations=[CommitOperationAdd(path_in_repo=remote,path_or_fileobj=data)],
        commit_message='Add bounded hourly research observations (read-only market APIs)')
    after = api.dataset_info(REPO,revision=result.oid,files_metadata=True)
    entry = next(f for f in after.siblings if f.rfilename==remote)
    matched = entry.size==len(data) and (
        entry.lfs.sha256==digest if entry.lfs else entry.blob_id==hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest())
    def identities(snapshot):
        return {f.rfilename:(f.blob_id,f.size,f.lfs.sha256 if f.lfs else None)
                for f in snapshot.siblings if f.rfilename != remote}
    if not matched or not after.private or identities(info) != identities(after):
        raise RuntimeError('Published snapshot verification failed')
    return dict(repo=REPO,commit=result.oid,path=remote,bytes=len(data),sha256=digest,
                verified=True,existing_files_unchanged=True,**budget)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publish',action='store_true')
    parser.add_argument('--output',type=Path,default=Path('research-review'))
    args=parser.parse_args()
    now=datetime.now(timezone.utc)
    run_id=os.environ.get('GITHUB_RUN_ID',now.strftime('%Y%m%dT%H%M%S%f'))
    cycle=now.strftime('%Y-%m-%dT%H%M%S%fZ')
    remote=f'{DATASET}/{now:%Y/%m/%d}/{cycle}-{run_id}.parquet'
    # Refuse a full day's exhausted budget before making any market-data calls.
    api=None
    if args.publish:
        from huggingface_hub import HfApi
        api=HfApi(token=os.environ['HF_TOKEN'])
        check_budget(api.dataset_info(REPO,files_metadata=True),remote,FILE_BUDGET)
    rows=[dict(row,schema_version=1,run_id=run_id,cycle_id=cycle) for row in collect()]
    args.output.mkdir(parents=True,exist_ok=True)
    file=args.output/'observations.parquet'
    pq.write_table(pa.Table.from_pylist(rows,schema=SCHEMA),file,compression='zstd',compression_level=6)
    health={source:{'rows':sum(r['source']==source for r in rows),
                    'error_rows':sum(r['source']==source and bool(r.get('error')) for r in rows)}
            for source in sorted({r['source'] for r in rows})}
    report={'bytes':file.stat().st_size,'rows':len(rows),'health':health,
            'projected_bytes_per_365_days_at_24_runs':file.stat().st_size*24*365,
            'daily_cap':DAY_BUDGET,'max_file_bytes':FILE_BUDGET,'remote_path':remote}
    (args.output/'summary.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report),flush=True)
    if api:
        receipt=publish(api,file,remote)
        (args.output/'receipt.json').write_text(json.dumps(receipt,indent=2))
        print(json.dumps(receipt),flush=True)
    if any(v['error_rows'] for v in health.values()):
        print('::warning::Some research sources failed; failed attempts were preserved')
    return 0 if any(not r.get('error') and r['kind'] in ('book','spot_book','indicative_yield') for r in rows) else 1


if __name__=='__main__':
    raise SystemExit(main())

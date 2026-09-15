"""Publish dataset-card edits as single-file, parent-checked Hugging Face commits."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re

from src.ops.hf_push import OWNER, PRODUCTS, _card
from src.ops.dataset_names import PUBLIC_NAMES, public_configs


def rewrite_card(existing: bytes, name: str) -> bytes:
    """Update display names and table configurations while retaining unrelated metadata."""
    import yaml

    original = existing.decode('utf-8')
    match = re.match(r'\A---\r?\n(.*?)\r?\n---(?:\r?\n|\Z)', original, re.S)
    if match is None:
        raise ValueError(f'{name}: missing YAML metadata; refusing to replace card')
    front = match.group(1)
    before = yaml.safe_load(front)
    if not isinstance(before, dict) or before.get('license') != 'odc-by':
        raise ValueError(f'{name}: unexpected metadata; review before publishing')
    title = json.dumps(PRODUCTS[name]['pretty'])
    front, count = re.subn(r'^pretty_name:.*$', lambda _: f'pretty_name: {title}', front, flags=re.M)
    if count != 1:
        raise ValueError(f'{name}: expected one display title')
    configs = public_configs(PRODUCTS[name])
    if 'configs' in before and before['configs'] not in (configs, public_configs(PRODUCTS[name], legacy_paths=True)):
        raise ValueError(f'{name}: existing configurations differ; refusing to replace them')
    updated = dict(before, pretty_name=PRODUCTS[name]['pretty'], configs=configs)
    front = yaml.safe_dump(updated, sort_keys=False).rstrip()
    after = yaml.safe_load(front)
    allowed = {'pretty_name', 'configs'}
    if {k: v for k, v in before.items() if k not in allowed} != {k: v for k, v in after.items() if k not in allowed}:
        raise ValueError(f'{name}: unrelated metadata changed')
    generated = _card(name, PRODUCTS[name])
    body = generated.split('\n---\n', 1)[1].lstrip('\n')
    for table in PRODUCTS[name]['datasets']:
        if f'`{PUBLIC_NAMES[table]}`' not in body:
            raise ValueError(f'{name}: missing table description: {table}')
    return ('---\n' + front + '\n---\n\n' + body).encode('utf-8')


def file_identities(info) -> dict:
    """Capture immutable Git/LFS identities for every file other than the card."""
    result = {}
    for entry in info.siblings:
        if entry.rfilename == 'README.md':
            continue
        if entry.blob_id is None or entry.size is None:
            raise ValueError('Repository file metadata is incomplete')
        lfs = asdict(entry.lfs) if entry.lfs is not None else None
        result[entry.rfilename] = {'blob_id': entry.blob_id, 'size': entry.size, 'lfs': lfs}
    return result


def validate_config_files(info, name: str):
    """Each public configuration must select exactly its existing table's Parquet files."""
    from fnmatch import fnmatchcase

    paths = {entry.rfilename for entry in info.siblings}
    selected = set()
    for storage_name, config in zip(PRODUCTS[name]['datasets'], public_configs(PRODUCTS[name])):
        expected = {path for path in paths if path.startswith(PUBLIC_NAMES[storage_name] + '/') and path.endswith('.parquet')}
        pattern = config['data_files'][0]['path']
        matches = {path for path in paths if fnmatchcase(path, pattern)}
        if not matches or matches != expected or selected.intersection(matches):
            raise ValueError(f'{name}: invalid file selection for {config["config_name"]}')
        selected.update(matches)
    if selected != {path for path in paths if path.endswith('.parquet')}:
        raise ValueError(f'{name}: some Parquet files are not covered by the named tables')


def publish_one(api, repo: str, before, card: bytes, report: Path):
    """The only write operation in this module is an addition/replacement of README.md."""
    from huggingface_hub import CommitOperationAdd

    identities = file_identities(before)
    if before.private:
        raise ValueError(f'{repo}: expected an existing public sample repository')
    if not any(item.rfilename == 'README.md' for item in before.siblings):
        raise ValueError(f'{repo}: existing README.md not found')
    operation = CommitOperationAdd(path_in_repo='README.md', path_or_fileobj=card)
    commit = api.create_commit(
        repo_id=repo,
        repo_type='dataset',
        operations=[operation],
        parent_commit=before.sha,
        commit_message='Use descriptive dataset table names and update loading examples',
    )
    # Pin verification to our commit so concurrent collection cannot affect the comparison.
    after = api.dataset_info(repo, revision=commit.oid, files_metadata=True)
    receipt = {
        'repo': repo, 'parent': before.sha, 'commit': commit.oid,
        'card_sha256': hashlib.sha256(card).hexdigest(),
        'non_card_files': len(identities),
        'non_card_files_unchanged': file_identities(after) == identities,
        'visibility_unchanged': after.private == before.private,
    }
    report.write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    if not receipt['non_card_files_unchanged'] or not receipt['visibility_unchanged']:
        raise RuntimeError(f'{repo}: verification failed; stopped subsequent card updates')
    return receipt


def main():
    from huggingface_hub import HfApi, hf_hub_download

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publish', action='store_true', help='Commit README.md edits after preflight')
    parser.add_argument('--output', type=Path, default=Path('card-review'))
    args = parser.parse_args()
    api = HfApi(token=os.environ['HF_TOKEN'])
    plans = []
    # Preflight and back up every card before submitting any write.
    for name in PRODUCTS:
        repo = f'{OWNER}/{name}'
        info = api.dataset_info(repo, files_metadata=True)
        if info.private:
            raise ValueError(f'{repo}: private repository is outside this update')
        identities = file_identities(info)
        validate_config_files(info, name)
        path = hf_hub_download(repo, 'README.md', repo_type='dataset', revision=info.sha, token=api.token)
        before = Path(path).read_bytes()
        card = rewrite_card(before, name)
        dest = args.output / name
        dest.mkdir(parents=True, exist_ok=True)
        (dest / 'README.before.md').write_bytes(before)
        (dest / 'README.after.md').write_bytes(card)
        (dest / 'files.before.json').write_text(json.dumps(identities, indent=2), encoding='utf-8')
        (dest / 'parent.txt').write_text(info.sha, encoding='utf-8')
        if card != before:
            plans.append((repo, info, card, dest))
        print(f'Prepared {repo}: README.md only; {len(identities)} other files', flush=True)
    print(f'{len(plans)} cards need an update', flush=True)
    if args.publish:
        for repo, info, card, dest in plans:
            receipt = publish_one(api, repo, info, card, dest / 'receipt.json')
            print(json.dumps(receipt), flush=True)


if __name__ == '__main__':
    main()

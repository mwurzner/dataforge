"""Rename public table directories by copying, verifying, then removing old paths."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from src.ops.dataset_names import PUBLIC_NAMES, RESTORE_TAG
from src.ops.hf_cards import file_identities, rewrite_card, validate_config_files
from src.ops.hf_push import OWNER, PRODUCTS


def plan(info, name):
    if name not in PRODUCTS or info.private:
        raise ValueError('Only the existing public products may be renamed')
    files = file_identities(info)
    mapping = {old: PUBLIC_NAMES[old] for old in PRODUCTS[name]['datasets']}
    moves = {}
    expected = dict(files)
    for path, identity in files.items():
        folder, separator, tail = path.partition('/')
        if folder in mapping and separator:
            target = mapping[folder] + '/' + tail
            if target in files and files[target] != identity:
                raise ValueError(f'{name}: destination collision: {target}')
            moves[path] = target
            expected[target] = identity
            del expected[path]
        elif path.endswith('.parquet') and folder not in mapping.values():
            raise ValueError(f'{name}: unrecognized data directory: {folder}')
    for folder in mapping.values():
        if not any(p.startswith(folder + '/') and p.endswith('.parquet') for p in expected):
            raise ValueError(f'{name}: missing table: {folder}')
    return files, moves, expected


def migrate(api, name, before, card, output):
    from huggingface_hub import CommitOperationAdd, CommitOperationCopy, CommitOperationDelete

    repo = f'{OWNER}/{name}'
    files, moves, expected = plan(before, name)
    # A tag keeps the original layout reachable. Never replace an existing restore tag.
    tags = {tag.name for tag in api.list_repo_refs(repo, repo_type='dataset').tags}
    if RESTORE_TAG not in tags:
        api.create_tag(repo, tag=RESTORE_TAG, revision=before.sha, repo_type='dataset',
                       tag_message='Original public file layout before descriptive directory names')
    copied = dict(files)
    copies = []
    for source, target in moves.items():
        copied[target] = files[source]
        if target not in files:
            copies.append(CommitOperationCopy(src_path_in_repo=source, path_in_repo=target,
                                              src_revision=before.sha))
    parent = before.sha
    if copies:
        commit = api.create_commit(repo, repo_type='dataset', operations=copies,
                                   parent_commit=parent, commit_message='Copy data to descriptive table directories')
        parent = commit.oid
    copy_info = api.dataset_info(repo, revision=parent, files_metadata=True)
    if copy_info.private != before.private or file_identities(copy_info) != copied:
        raise RuntimeError(f'{repo}: copy verification failed; no original paths removed')
    (output / 'copy.json').write_text(json.dumps({'commit': parent, 'verified': True}, indent=2))
    # Exact source paths only; the new copy of every removed file has been verified.
    operations = [CommitOperationDelete(path_in_repo=source) for source in moves]
    operations.append(CommitOperationAdd(path_in_repo='README.md', path_or_fileobj=card))
    commit = api.create_commit(repo, repo_type='dataset', operations=operations,
                               parent_commit=parent, commit_message='Use descriptive folders and update dataset card paths')
    after = api.dataset_info(repo, revision=commit.oid, files_metadata=True)
    receipt = {'repo': repo, 'parent': before.sha, 'copy_commit': parent, 'commit': commit.oid,
               'restore_tag': RESTORE_TAG, 'renamed_files': len(moves),
               'non_card_files': len(expected), 'file_contents_preserved': file_identities(after) == expected,
               'visibility_unchanged': after.private == before.private}
    (output / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    if not receipt['file_contents_preserved'] or not receipt['visibility_unchanged']:
        raise RuntimeError(f'{repo}: final verification failed; stopped further migrations')
    validate_config_files(after, name)
    return receipt


def main():
    from huggingface_hub import HfApi, hf_hub_download

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publish', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('folder-review'))
    args = parser.parse_args()
    api = HfApi(token=os.environ['HF_TOKEN'])
    plans = []
    # Preflight and save all inventories and cards before the first remote mutation.
    for name in PRODUCTS:
        repo = f'{OWNER}/{name}'
        info = api.dataset_info(repo, files_metadata=True)
        files, moves, expected = plan(info, name)
        old_card = Path(hf_hub_download(repo, 'README.md', repo_type='dataset', revision=info.sha,
                                       token=api.token)).read_bytes()
        card = rewrite_card(old_card, name)
        output = args.output / name
        output.mkdir(parents=True, exist_ok=True)
        for filename, value in [('files.before.json', files), ('files.expected.json', expected), ('moves.json', moves)]:
            (output / filename).write_text(json.dumps(value, indent=2), encoding='utf-8')
        (output / 'README.before.md').write_bytes(old_card)
        (output / 'README.after.md').write_bytes(card)
        (output / 'parent.txt').write_text(info.sha)
        plans.append((name, info, card, output))
        print(f'Prepared {repo}: {len(moves)} file paths to rename; {len(expected)} files retained', flush=True)
    if args.publish:
        for args_one in plans:
            print(json.dumps(migrate(api, *args_one)), flush=True)


if __name__ == '__main__':
    main()

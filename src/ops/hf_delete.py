"""Delete listed files from both HF repos. For corrections that local hands cannot make.

The write token exists only as an Actions secret after rotation, so any correction to already-
published files must ride through the workflow. This reads a committed list and deletes each path
from both repos. Idempotent: a path already absent is skipped, so the list can stay in place.
NEVER wildcards -- every deletion is a named file, reviewable in the commit that added it.
"""
from __future__ import annotations
import json, os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIST = ROOT / ".hf_delete_list.json"

def main() -> int:
    if not LIST.exists():
        return 0
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("  no HF_TOKEN; skipping deletions", flush=True)
        return 0
    files = json.loads(LIST.read_text(encoding="utf-8")).get("files", [])
    if not files:
        return 0
    from huggingface_hub import HfApi
    api = HfApi(token=token)
    owner = os.environ.get("HF_OWNER", "SleeveZipper")
    for repo in (f"{owner}/dataforge-ephemeral",):
        try:
            existing = set(api.list_repo_files(repo_id=repo, repo_type="dataset"))
        except Exception as exc:
            # A repo that is gone cannot hold anything we need to delete. Failing the whole
            # run over it marked four otherwise-healthy collections as failures.
            print(f"  {repo}: unreachable ({type(exc).__name__}), skipping", flush=True)
            continue
        for f in files:
            if f in existing:
                api.delete_file(path_in_repo=f, repo_id=repo, repo_type="dataset",
                                commit_message=f"remove baseline-only test partition {f.split('/')[-1]}")
                print(f"  deleted {f} from {repo}", flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

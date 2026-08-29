"""Push this run's partitions to the data repo, surviving any concurrent change to it.

WHY THIS EXISTS. On 2026-08-26 a full 5.5h collection window was LOST. The sequence:

    1. a hand-made commit to the data repo removed one junk partition while the run was live
    2. the run's own harmonise pass rewrote that same partition on the runner
    3. `git push` was rejected (remote ahead), and the `pull --rebase` fallback hit a
       modify/delete conflict, leaving the checkout detached with nothing pushed
    4. the HuggingFace push failed the same run on a rotated-out token
    5. both destinations were therefore down for that window, and an ephemeral window cannot
       be re-collected

Rebasing was the wrong model. Our commit is not a change to shared history that must be replayed
in order; it is A SET OF NEW FILES that belong on top of whatever the remote now says. So on
rejection this takes the remote's state wholesale and re-applies only the files this run wrote:

    * files this run produced      -> ours, always
    * anything else the remote changed or deleted -> theirs, without argument

That cannot conflict, because the two sides are never claiming the same file for different
reasons. The one case it deliberately does not handle is a remote that has legitimately rewritten
one of THIS run's partitions, which cannot happen: partitions are keyed by run id.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
ATTEMPTS = 4


def _git(*args, check=True, cwd=None):
    r = subprocess.run(["git", *args], cwd=str(cwd or DATA),
                       capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {r.stderr.strip()[:300]}")
    return r


def _changed_files() -> list[str]:
    """Every changed FILE, with untracked directories expanded.

    THE BUG THIS FIXES, which cost a real collection window. `git status --porcelain` reports a
    brand-new dataset as an untracked DIRECTORY -- `?? e21_btc_block_propagation/` -- not as its
    individual files. The restore loop below only copies entries where `is_file()` is true, so on
    a rejected push the reset wiped the new directory and nothing put it back. E21 wrote 946 rows
    on the runner, the push was rejected, it retried, and the dataset simply was not there
    afterwards. Datasets that already existed survived because their paths were listed per file.

    Nothing errored. The run reported success and pushed 220 partitions; one dataset was just
    missing from all of them.
    """
    r = _git("status", "--porcelain")
    out = []
    for line in r.stdout.splitlines():
        if not line.strip():
            continue
        path = line[3:].strip().strip('"')
        # Renames appear as "old -> new"; only the new side is ours to carry.
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        full = DATA / path
        if full.is_dir():
            for f in sorted(full.rglob("*")):
                if f.is_file():
                    out.append(str(f.relative_to(DATA)).replace("\\", "/"))
        else:
            out.append(path)
    return out


def main() -> int:
    if not (DATA / ".git").exists():
        print("  data/ is not a git checkout; nothing to push", flush=True)
        return 0

    _git("config", "user.name", "dataforge[bot]")
    _git("config", "user.email", "dataforge@users.noreply.github.com")

    mine = _changed_files()
    if not mine:
        print("  no new partitions: the collector wrote nothing, which is itself a failure",
              flush=True)
        return 1
    print(f"  {len(mine)} changed paths to push", flush=True)

    stamp = os.environ.get("DF_RUN_ID", "")
    for attempt in range(1, ATTEMPTS + 1):
        _git("add", "-A")
        if _git("diff", "--cached", "--quiet", check=False).returncode != 0:
            _git("commit", "-m", f"ephemeral: {stamp}" if stamp else "ephemeral snapshot")
        push = _git("push", check=False)
        if push.returncode == 0:
            print(f"  pushed on attempt {attempt}", flush=True)
            return 0

        print(f"  push rejected (attempt {attempt}); re-applying our files onto the remote",
              flush=True)
        # Preserve exactly what this run produced, then take the remote wholesale.
        with tempfile.TemporaryDirectory() as tmp:
            saved = []
            for rel in mine:
                src = DATA / rel
                if src.is_file():
                    dst = Path(tmp) / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    saved.append(rel)
            _git("fetch", "origin", check=False)
            # Any in-progress rebase from an older workflow version must be cleared first, or
            # the checkout stays detached and every later command misbehaves.
            _git("rebase", "--abort", check=False)
            _git("reset", "--hard", "origin/main", check=False)
            _git("checkout", "main", check=False)
            _git("reset", "--hard", "origin/main", check=False)
            for rel in saved:
                dst = DATA / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(Path(tmp) / rel, dst)
            print(f"    restored {len(saved)} of our files onto the remote state", flush=True)

    print("  !! could not push after retries -- HuggingFace remains the other destination",
          flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""
Directory Indexer - Scheduler Starter Template

Learner task:
- Adapt choose_next_job() (and optionally on_job_feedback()) to implement ONE scheduler:
  FCFS, SJF, Round Robin, MLQ, or MLFQ.

What this script does:
- Walks a directory and creates "jobs" (each job = one file path to index)
- Indexes each job: metadata + basic permissions (POSIX-style)
- Writes results to JSON Lines (.jsonl)
"""

from __future__ import annotations

import os
import json
import stat
import mimetypes
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Any, List, Optional


# -----------------------------
# Job model (students may extend)
# -----------------------------
@dataclass
class Job:
    path: Path
    arrival: int            # "arrival time" in ticks (just a counter in this simulation)
    est_cost: int           # estimated cost (used for SJF/MLFQ ideas)
    remaining: int = 1      # for RR/MLFQ: remaining "work units" (simple abstraction)
    queue_level: int = 0    # for MLQ/MLFQ: which queue the job is in


# -----------------------------
# Indexing work (the "CPU burst")
# -----------------------------
def scan_one_path(path: Path) -> Dict[str, Any]:
    """
    Collect file details + basic metadata + basic permissions for ONE filesystem entry.
    Keep it simple: permissions are POSIX mode bits + uid/gid where available.
    """
    rec: Dict[str, Any] = {
        "path": str(path),
        "name": path.name
    }

    try:
        st = path.lstat()  # don't follow symlinks
        
        # bool values
        rec["is_file"] = stat.S_ISREG(st.st_mode)
        rec["is_dir"] = stat.S_ISDIR(st.st_mode)
        rec["is_symlink"] = stat.S_ISLNK(st.st_mode)

        rec["size_bytes"] = st.st_size
        rec["mtime"] = st.st_mtime # time last modified
        rec["atime"] = st.st_atime # time last accessed
        rec["ctime"] = st.st_ctime # time last metadata change

        rec["extension"] = path.suffix.lower()
        mime, _ = mimetypes.guess_type(str(path)) # e.g. "text/plain" or "image/png"
        rec["mime_guess"] = mime

        # Basic permissions snapshot
        rec["mode_octal"] = oct(stat.S_IMODE(st.st_mode)) # permission bits
        rec["uid"] = getattr(st, "st_uid", None)
        rec["gid"] = getattr(st, "st_gid", None)

    except PermissionError as e:
        rec["error"] = f"PermissionError: {e}"
    except FileNotFoundError as e:
        rec["error"] = f"FileNotFoundError: {e}"
    except OSError as e:
        rec["error"] = f"OSError: {e}"

    return rec


# -----------------------------
# Job creation (workload)
# -----------------------------
def build_jobs(root: Path) -> List[Job]:
    """
    Build jobs from all files under root.
    We add simple fields that schedulers can use:
    - arrival: increasing counter (simulates arrival time)
    - est_cost: cheap estimate (based on size buckets; students can improve)
    - remaining: work units for RR/MLFQ (default 1, but could be >1 for big files)
    """
    jobs: List[Job] = []
    tick = 0

    for dirpath, _, filenames in os.walk(root):
        # sort filenames if needed
        # filenames.sort()
        d = Path(dirpath)
        for fn in filenames:
            full_path = d / fn
            tick += 1 # so each file has diff arrival time

            # Simple estimate: larger files => higher cost (bucketed)
            try:
                size = full_path.lstat().st_size if stat.S_ISREG(full_path.lstat().st_mode) else 0
                # uses st_size only if a file, instead of path size if symlink
            except OSError:
                size = 0

            # TODO: Improve this
            if size < 100_000:         # < 100 KB
                est_cost = 1
            elif size < 10_000_000:    # < 10 MB
                est_cost = 2
            else:
                est_cost = 3

            jobs.append(Job(path=full_path, arrival=tick, est_cost=est_cost, remaining=est_cost))

    return jobs


# -----------------------------
# Scheduler hooks (students adapt)
# -----------------------------
def choose_next_job(ready: Deque[Job], tick: int) -> Optional[Job]:
    """
    STUDENT TASK: Replace this logic to implement a scheduler.

    Current behaviour: FCFS (pop from front)

    Ideas:
    - FCFS:         return ready.popleft()
    - SJF:          choose job with smallest est_cost (then remove it)
    - Round Robin:  pop left, run 1 unit, if remaining>0 append back
    - MLQ:          maintain multiple queues based on queue_level
    - MLFQ:         demote if it uses full quantum; promote/boost occasionally
    """
    if not ready:
        return None
    return ready.popleft()


def on_job_feedback(job: Job, record: Dict[str, Any]) -> None:
    """
    Optional STUDENT TASK:
    Use job results to change scheduling behaviour.
    Example MLFQ ideas:
    - If record has "error": demote (slow/problematic paths)
    - If size_bytes huge: demote
    - If job finishes quickly: keep high priority
    """
    # Default: do nothing
    pass


# -----------------------------
# Simulation loop (runs "scheduler")
# -----------------------------
def run_indexer(root: Path, output_jsonl: Path) -> None:
    # Create jobs and load into a ready queue
    jobs = build_jobs(root)

    # In this simple model, all jobs are "ready" immediately.
    # Students can extend this by using arrival times more realistically.
    ready: Deque[Job] = deque(sorted(jobs, key=lambda j: j.arrival))

    tick = 0

    with output_jsonl.open("w", encoding="utf-8") as f:
        while ready:
            tick += 1

            job = choose_next_job(ready, tick)
            if job is None:
                continue

            # "Run" the job: do one index operation
            record = scan_one_path(job.path)

            # Helpful fields to see scheduling outcomes
            record["arrival"] = job.arrival
            record["est_cost"] = job.est_cost
            record["queue_level"] = job.queue_level
            record["tick_ran"] = tick

            # Feedback hook (MLFQ-style adaptations)
            on_job_feedback(job, record)

            # Write one record per line (easy to parse and analyse)
            f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    root_folder = Path(r"")
    output_file = Path.cwd() / "outputs" / "index_results.jsonl"
    run_indexer(root_folder, output_file)
    print(f"Done. Wrote: {output_file.resolve()}")

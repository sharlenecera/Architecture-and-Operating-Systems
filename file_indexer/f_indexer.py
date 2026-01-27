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
# import cmd
import json
import stat
import mimetypes
import hashlib
import threading
import time
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from queue import Queue
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
def scan_one_path(
    path: Path,
) -> Dict[str, Any]:
    """
    Collect file details + basic metadata + basic permissions for ONE filesystem entry.
    Keep it simple: permissions are POSIX mode bits + uid/gid where available.
    """
    rec: Dict[str, Any] = {
        "path": str(path),
        "name": path.name,
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
def build_jobs(
    root: Path,
    greater_than: int = 0,
) -> List[Job]:
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

            if size < greater_than:
                continue

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
def choose_next_job(
    ready: list[Job],
    tick: int,
) -> Optional[Job]:
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
    return ready.pop(0)


def on_job_feedback(
    job: Job,
    record: Dict[str, Any],
) -> None:
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
def run_indexer(
    root: Path = Path(r""),
    output_jsonl: Path = Path("index_results.jsonl"),
    greater_than: int = 0,
) -> None:
    start = time.perf_counter()

    # Create jobs and load into a ready queue
    jobs = build_jobs(root, greater_than)

    # In this simple model, all jobs are "ready" immediately.
    # Students can extend this by using arrival times more realistically.
    ready: list[Job] = sorted(jobs, key=lambda j: j.arrival)

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
        end = time.perf_counter()
    print(f"Control indexing for '{root or "root"}' took {end - start: 0.4f} seconds")


def run_indexer_threaded(
    root: Path = Path(r""),
    output_jsonl: Path = Path("index_results_threaded.jsonl"),
    greater_than: int = 0,
    max_workers: int = 8,
) -> None:
    start = time.perf_counter()

    jobs = build_jobs(root, greater_than)
    ready: list[Job] = sorted(jobs, key=lambda j: j.arrival)

    queue: Queue[Job] = Queue()
    for job in ready:
        queue.put(job)

    write_lock = threading.Lock()
    tick_lock = threading.Lock()
    tick = 0 # shared counter

    def worker():
        nonlocal tick

        while True:
            try:
                job = queue.get_nowait() # choose next job
            except Exception:
                break # if queue empty

            try:
                record = scan_one_path(job.path)
                record["arrival"] = job.arrival
                record["est_cost"] = job.est_cost
                record["queue_level"] = job.queue_level
                with tick_lock:
                    tick += 1
                    record["tick_ran"] = tick
                with write_lock:
                    f.write(json.dumps(record) + "\n")
            except Exception as e:
                with write_lock:
                    f.write(json.dumps({"path": str(job.path), "error": f"ThreadError: {e}"}) + "\n")
                    print(f"Path '{job.path}' raised error: {e}")
            finally:
                queue.task_done()
                # on_job_feedback(job, record) # TODO
    
    with output_jsonl.open("w", encoding="utf-8") as f:
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(max_workers)]
        for thread in threads:
            thread.start()
        queue.join() # wait until all jobs completed
        for thread in threads:
            thread.join(timeout=0.1) # let threads exit cleanly

    end = time.perf_counter()
    print(f"Threaded indexing for '{root or "root"}' took {end - start: 0.4f} seconds (max workers: {max_workers})")

# -----------------------------
# CLI (first version)
# -----------------------------
# class FileIndexerCLI(cmd.Cmd):
#     prompt = ">> "
#     welcome_msg = "Welcome to Sharlene's File Indexer CLI> Type \"help\" for available commands."

#     def preloop(self):
#         """Print welcome message"""
#         print(self.welcome_msg)

#     def do_index(self, line):
#         """Create the index results file in outputs folder"""
#         root_folder = Path(r"")
#         # output_file = Path.cwd() / "outputs" / "index_results.jsonl"
#         output_file = Path.cwd() / "temp" / "index_results.jsonl"
#         run_indexer(root_folder, output_file)
#         print(f"Done. Wrote: {output_file.resolve()}")

#     def do_find_bigger_than(self, line):
#         """Find files bigger than x MB"""
#         print('finding')

#     def do_q(self, line):
#         """Exit the CLI"""
#         return True


# -----------------------------
# Indexer tools
# -----------------------------
def get_files_greater_than(
    root: Path,
    output_jsonl: Path,
    number: int,
) -> None:
    index_results = run_indexer(root, output_jsonl, greater_than=number)


def hash_file(
    file_path: str,
    hash_type: str,
) -> Optional[str]:
    if hash_type.lower() not in hashlib.algorithms_guaranteed:
        raise ValueError("Invalid hash type.")
    hash_function = hashlib.new(hash_type.lower())
    try:
        # open in binary read mode
        with open(file_path, "rb") as file:
            # read the file in 8192 byte chunks
            while chunk := file.read(8192):
                hash_function.update(chunk)
        return hash_function.hexdigest()
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except:
        print(f"Other error occurred when hashing file.")


if __name__ == "__main__":
    # FileIndexerCLI().cmdloop()
    # root_folder = Path(r"")
    # # output_file = Path.cwd() / "outputs" / "index_results.jsonl"
    # output_file = Path.cwd() / "temp" / "index_results.jsonl"
    # run_indexer(root_folder, output_file)
    # print(f"Done. Wrote: {output_file.resolve()}")
    run_indexer()
    run_indexer_threaded(max_workers=4)
    
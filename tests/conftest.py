import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent
APP_MOD = [sys.executable, "-m", "queuectl"]


def run(args, env, timeout=15):
    return subprocess.run(
        APP_MOD + args,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


@pytest.fixture()
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


def list_jobs(env, state=None):
    args = ["list", "--json"]
    if state:
        args = ["list", "--state", state, "--json"]
    out = run(args, env).stdout.strip()
    return json.loads(out)


def start_worker(env, count=1, logfile=None):
    f = open(logfile, "w") if logfile else subprocess.DEVNULL
    return subprocess.Popen(
        APP_MOD + ["worker", "start", "--count", str(count)],
        stdout=f,
        stderr=subprocess.STDOUT,
        env=env,
    )


def wait_for_state(env, job_id, state, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = list_jobs(env)
        for j in jobs:
            if j["id"] == job_id and j["state"] == state:
                return j
        time.sleep(0.2)
    jobs = list_jobs(env)
    raise AssertionError(f"job {job_id} did not reach state {state} in time; last seen: {jobs}")

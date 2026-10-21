#!/usr/bin/env python3
"""Run a verified workload, SIGKILL a busy kind container, and verify recovery."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("--kubeconfig", required=True, help="explicit isolated lab kubeconfig")
parser.add_argument("--kind-node", default="queue-lab-control-plane", help="Docker container for the isolated kind node")
parser.add_argument("--loadtest", default="artifacts/load/loadtest")
parser.add_argument("--url", default="http://localhost:18080")
parser.add_argument("--output", default="artifacts/kubernetes-crash.json")
args = parser.parse_args()
if not os.environ.get("DATABASE_URL"):
    parser.error("DATABASE_URL must point to the Kubernetes PostgreSQL port-forward")
output = Path(args.output)
output.parent.mkdir(parents=True, exist_ok=True)
kubectl = ["kubectl", "--kubeconfig", args.kubeconfig, "-n", "queue-lab"]
events = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
with output.open("w") as report:
    load = subprocess.Popen([args.loadtest, "-url", args.url, "-requests", "4000",
                             "-unique", "2000", "-concurrency", "20", "-workers", "3",
                             "-timeout", "4m"], stdout=report)
    try:
        deadline = time.monotonic() + 15
        pod = ""
        while time.monotonic() < deadline and load.poll() is None:
            pod = subprocess.check_output(kubectl + ["exec", "postgres-0", "--", "psql",
                "-U", "queue", "-d", "queue", "-At", "-c",
                "SELECT worker_id FROM job_attempts WHERE status='running' ORDER BY id DESC LIMIT 1"], text=True).strip()
            if pod:
                break
            time.sleep(.1)
        if not pod:
            raise RuntimeError("no active worker observed before the workload drained")
        events["killed_pod"] = pod
        info = json.loads(subprocess.check_output(kubectl + ["get", "pod", pod, "-o", "json"], text=True))
        status = next(item for item in info["status"]["containerStatuses"] if item["name"] == "worker")
        container_id = status["containerID"].removeprefix("containerd://")
        if len(container_id) != 64 or any(c not in "0123456789abcdef" for c in container_id):
            raise RuntimeError("unexpected container runtime identity")
        events["kill_requested_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        events["crash_method"] = "containerd SIGKILL through the isolated kind node"
        events["container_id"] = container_id
        subprocess.run(["docker", "exec", args.kind_node, "ctr", "-n", "k8s.io", "tasks", "kill", "--signal", "SIGKILL", container_id], check=True)
        if load.wait(timeout=250) != 0:
            raise RuntimeError("load verification failed; inspect the JSON report")
    finally:
        if load.poll() is None:
            load.kill()
            load.wait()
result = json.loads(output.read_text())
# UUID output from our generator is validated before it is used in SQL text.
import uuid
run_id = str(uuid.UUID(result["run"]))
events["attempt_history"] = subprocess.check_output(kubectl + ["exec", "postgres-0", "--", "psql",
    "-U", "queue", "-d", "queue", "-At", "-c",
    "SELECT a.status,count(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id "
    f"WHERE j.idempotency_key LIKE 'load-{run_id}-%' GROUP BY a.status ORDER BY a.status"], text=True).strip()
events["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
Path(str(output) + ".events.json").write_text(json.dumps(events, indent=2) + "\n")
if "interrupted|" not in events["attempt_history"] and "skipped|" not in events["attempt_history"]:
    raise RuntimeError("workload drained, but no interrupted/skipped delivery was observed")

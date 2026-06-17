#!/usr/bin/env python3
"""向 Master HTTP API 提交 MapReduce 任务并等待完成。"""

import argparse
import json
import sys
import time

try:
    import requests
except ImportError:
    print("请先安装 requests: pip install requests", file=sys.stderr)
    sys.exit(1)


def submit_job(master_url, input_files, map_func, reduce_func, combine_func="", n_reduce=3, split_size=0, work_dir="mr-work"):
    payload = {
        "input_files": input_files,
        "n_reduce": n_reduce,
        "map_func": map_func,
        "reduce_func": reduce_func,
        "combine_func": combine_func,
        "split_size": split_size,
        "work_dir": work_dir,
    }
    resp = requests.post(f"{master_url}/api/job", json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["job_id"]


def poll_status(master_url, job_id):
    resp = requests.get(f"{master_url}/api/status", params={"job": job_id}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_result(master_url, job_id):
    resp = requests.get(f"{master_url}/api/result", params={"job": job_id}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Submit MapReduce job to Master")
    parser.add_argument("--master", default="http://localhost:8081", help="Master HTTP URL")
    parser.add_argument("--input", nargs="+", required=True, help="Input files")
    parser.add_argument("--map", default="wordcount_map", dest="map_func")
    parser.add_argument("--reduce", default="wordcount_reduce", dest="reduce_func")
    parser.add_argument("--combine", default="wordcount_combine", dest="combine_func")
    parser.add_argument("--nreduce", type=int, default=3)
    parser.add_argument("--split", type=int, default=0)
    parser.add_argument("--workdir", default="mr-work")
    args = parser.parse_args()

    job_id = submit_job(
        args.master, args.input, args.map_func, args.reduce_func,
        args.combine_func, args.nreduce, args.split, args.workdir,
    )
    print(f"Job submitted: {job_id}")

    while True:
        status = poll_status(args.master, job_id)
        print(f"  state={status['state']} map={status['map_completed']}/{status['map_total']} "
              f"reduce={status['reduce_completed']}/{status['reduce_total']}")
        if status["state"] == "completed":
            break
        if status["state"] == "failed":
            print(f"Job failed: {status.get('error', '')}", file=sys.stderr)
            sys.exit(1)
        if status["state"] == "recoverable":
            print("Job recoverable, waiting for master recovery...")
        time.sleep(2)

    result = get_result(args.master, job_id)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""爬虫数据流水线演示: 模拟爬虫产出 → 提交 MapReduce 清洗 → 展示结果。"""

import json
import os
import sys
import time

try:
    import requests
except ImportError:
    print("请先安装 requests: pip install requests", file=sys.stderr)
    sys.exit(1)

MASTER_URL = os.environ.get("MR_MASTER_HTTP", "http://localhost:8081")
SAMPLE_FILE = os.path.join(os.path.dirname(__file__), "..", "testdata", "crawler_sample", "data.jsonl")
WORK_DIR = "mr-work-crawl"


def simulate_crawl(output_path):
    """模拟爬虫: 复制样本 JSONL 到输出路径。"""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(SAMPLE_FILE, "r", encoding="utf-8") as src:
        data = src.read()
    with open(output_path, "w", encoding="utf-8") as dst:
        dst.write(data)
    print(f"[crawler] 产出数据: {output_path} ({len(data)} bytes)")


def submit_clean_job(input_file):
    payload = {
        "input_files": [input_file],
        "n_reduce": 2,
        "map_func": "crawl_clean_map",
        "reduce_func": "crawl_clean_reduce",
        "combine_func": "",
        "split_size": 0,
        "work_dir": WORK_DIR,
    }
    resp = requests.post(f"{MASTER_URL}/api/job", json=payload, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    print(f"[pipeline] 提交清洗任务: {job_id}")

    while True:
        status = requests.get(f"{MASTER_URL}/api/status", params={"job": job_id}, timeout=10).json()
        if status["state"] == "completed":
            break
        time.sleep(2)

    result = requests.get(f"{MASTER_URL}/api/result", params={"job": job_id}, timeout=10).json()
    return result


def post_process(result):
    """Python 后处理: 读取并展示清洗结果。"""
    print("\n[pipeline] 清洗结果:")
    for out_file in result.get("output_files", []):
        if not os.path.exists(out_file):
            continue
        with open(out_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t", 1)
                if len(parts) == 2:
                    domain, data = parts
                    try:
                        parsed = json.loads(data)
                        print(f"  域名: {domain}")
                        print(f"    总数: {parsed.get('count')}, 去重: {parsed.get('unique_count')}")
                        preview = parsed.get("merged_text", "")[:120]
                        print(f"    预览: {preview}...")
                    except json.JSONDecodeError:
                        print(f"  {line}")


def main():
    crawl_output = "testdata/crawler_sample/crawled.jsonl"
    simulate_crawl(crawl_output)
    result = submit_clean_job(os.path.abspath(crawl_output))
    post_process(result)


if __name__ == "__main__":
    main()

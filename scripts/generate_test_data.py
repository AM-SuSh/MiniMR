#!/usr/bin/env python3
"""生成中文测试数据。"""

import json
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "testdata", "crawler_sample", "generated.jsonl")

SAMPLES = [
    {
        "url": "https://tech.example.cn/ai/001",
        "html": "<html><body><h1>大语言模型</h1><p>Transformer 架构改变了 NLP 领域。</p></body></html>",
        "timestamp": "2026-05-24T08:00:00Z",
    },
    {
        "url": "https://tech.example.cn/ai/002",
        "html": "<div><p>Transformer 架构改变了 NLP 领域。</p><p>注意力机制是关键。</p></div>",
        "timestamp": "2026-05-24T08:30:00Z",
    },
    {
        "url": "https://edu.example.com/course/go",
        "html": "<article><h2>Go 并发编程</h2><p>goroutine 和 channel 是 Go 的核心特性。</p></article>",
        "timestamp": "2026-05-24T09:00:00Z",
    },
    {
        "url": "https://edu.example.com/course/mr",
        "html": "<p>MapReduce 适合大规模离线批处理。</p>",
        "timestamp": "2026-05-24T09:30:00Z",
    },
]

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
with open(OUTPUT, "w", encoding="utf-8") as f:
    for rec in SAMPLES:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print(f"Generated {len(SAMPLES)} records -> {OUTPUT}")

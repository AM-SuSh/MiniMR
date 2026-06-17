(function () {
  "use strict";

  const POLL_MS = 1500;

  const $ = (id) => document.getElementById(id);

  const els = {
    masterUrl: $("master-url"),
    jobId: $("job-id"),
    btnConnect: $("btn-connect"),
    btnLatest: $("btn-latest"),
    btnPause: $("btn-pause"),
    btnTheme: $("btn-theme"),
    emptyState: $("empty-state"),
    dashboard: $("dashboard"),
    liveDot: $("live-dot"),
    jobState: $("job-state"),
    heroJobLabel: $("hero-job-label"),
    jobIdDisplay: $("job-id-display"),
    pipeline: $("pipeline"),
    pipeMapPct: $("pipe-map-pct"),
    pipeMapBar: $("pipe-map-bar"),
    pipeShuffleLabel: $("pipe-shuffle-label"),
    pipeShuffleHint: $("pipe-shuffle-hint"),
    pipeShuffleConn: $("pipe-shuffle-conn"),
    reduceShuffleSummary: $("reduce-shuffle-summary"),
    reduceShuffleCount: $("reduce-shuffle-count"),
    reduceShuffleGrid: $("reduce-shuffle-grid"),
    reduceShuffleEmpty: $("reduce-shuffle-empty"),
    pipeReducePct: $("pipe-reduce-pct"),
    pipeReduceBar: $("pipe-reduce-bar"),
    pipeReduceConn: $("pipe-reduce-conn"),
    mapProgressText: $("map-progress-text"),
    reduceProgressText: $("reduce-progress-text"),
    slowstartStatus: $("slowstart-status"),
    slowstartDetail: $("slowstart-detail"),
    optSlowStatus: $("opt-slow-status"),
    optSlowMetric: $("opt-slow-metric"),
    optSlowBar: $("opt-slow-bar"),
    optSlowDetail: $("opt-slow-detail"),
    optShuffleRecords: $("opt-shuffle-records"),
    optShuffleSaved: $("opt-shuffle-saved"),
    optShuffleBefore: $("opt-shuffle-before"),
    optShuffleAfter: $("opt-shuffle-after"),
    optShuffleBar: $("opt-shuffle-bar"),
    optStreamBuffer: $("opt-stream-buffer"),
    optStreamRecords: $("opt-stream-records"),
    optStreamDetail: $("opt-stream-detail"),
    optFaultStatus: $("opt-fault-status"),
    optFaultChecks: $("opt-fault-checks"),
    optFaultDetail: $("opt-fault-detail"),
    jobConfig: $("job-config"),
    schedulingInsight: $("scheduling-insight"),
    workersGrid: $("workers-grid"),
    workersEmpty: $("workers-empty"),
    workerCount: $("worker-count"),
    historyCount: $("history-count"),
    jobHistory: $("job-history"),
    jobHistoryEmpty: $("job-history-empty"),
    mapTasks: $("map-tasks"),
    reduceTasks: $("reduce-tasks"),
    partitionGrid: $("partition-grid"),
    decisionsLog: $("decisions-log"),
    decisionsEmpty: $("decisions-empty"),
    decisionsCard: $("decisions-card"),
    decisionsCount: $("decisions-count"),
    pollStatus: $("poll-status"),
    lastUpdate: $("last-update"),
  };

  let polling = false;
  let paused = false;
  let timer = null;
  let decisionsJobId = "";
  let decisionsFingerprint = "";

  const stateLabels = {
    running: "作业运行中",
    completed: "作业已完成",
    failed: "作业失败",
    pending: "等待调度",
  };

  function defaultMasterURL() {
    const { protocol, hostname, port } = window.location;
    if (port === "8081" || !port) {
      return `${protocol}//${hostname}:8081`;
    }
    return `${protocol}//${hostname}${port ? ":" + port : ""}`;
  }

  els.masterUrl.value = defaultMasterURL();

  function apiBase() {
    return els.masterUrl.value.replace(/\/$/, "");
  }

  function pct(done, total) {
    if (!total) return 0;
    return Math.min(100, Math.round((done / total) * 100));
  }

  function formatTime(iso) {
    if (!iso) return "—";
    return new Date(iso).toLocaleTimeString("zh-CN", { hour12: false });
  }

  function formatDateTime(iso) {
    if (!iso) return "—";
    return new Date(iso).toLocaleString("zh-CN", { hour12: false });
  }

  function formatMs(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
  }

  function formatBytes(bytes) {
    if (!bytes) return "—";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let n = Number(bytes);
    let i = 0;
    while (n >= 1024 && i < units.length - 1) {
      n /= 1024;
      i++;
    }
    return `${n >= 10 || i === 0 ? n.toFixed(0) : n.toFixed(1)} ${units[i]}`;
  }

  function formatCount(n) {
    n = Number(n || 0);
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
  }

  function formatPercent(n) {
    if (!Number.isFinite(Number(n))) return "0%";
    return `${Number(n).toFixed(1)}%`;
  }

  function shortJobID(id) {
    if (!id) return "—";
    return id.length > 10 ? `${id.slice(0, 8)}…` : id;
  }

  function progressRatio(done, total) {
    return total ? Math.round((done / total) * 100) : 0;
  }

  function setLiveState(state) {
    if (!els.liveDot) return;
    els.liveDot.className = "live-dot";
    if (state) els.liveDot.classList.add(state);
  }

  function renderConfig(dl, rows) {
    dl.innerHTML = rows
      .map(
        ([label, value]) =>
          `<div class="row"><dt>${label}</dt><dd>${escapeHtml(String(value ?? "—"))}</dd></div>`
      )
      .join("");
  }

  function escapeHtml(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function partitionIndex(partitions) {
    const index = new Map();
    for (const p of partitions || []) {
      index.set(Number(p.reduce_id), p);
    }
    return index;
  }

  /**
   * Infer Reduce task phase from partition readiness:
   * - shuffle: in_progress + partition not fully ready (Worker polling)
   * - compute: in_progress + partition ready (streaming merge + reduce)
   */
  function analyzeReducePhases(reduceTasks, partitions) {
    const byReduce = partitionIndex(partitions);
    const phases = new Map();
    const shuffling = [];
    const computing = [];

    for (const task of reduceTasks || []) {
      if (task.state !== "in_progress") continue;
      const rid = Number(task.reduce_id ?? task.id);
      const part = byReduce.get(rid);
      const ready = Boolean(part?.ready);
      const mapsReady = Number(part?.maps_ready ?? 0);
      const mapsTotal = Number(part?.maps_total ?? 0);
      const phase = ready ? "compute" : "shuffle";
      const info = { task, rid, phase, mapsReady, mapsTotal, ready };
      phases.set(task.id, info);
      if (phase === "shuffle") shuffling.push(info);
      else computing.push(info);
    }

    return { phases, shuffling, computing };
  }

  function renderTaskCell(task, reducePhase) {
    const classes = ["task-cell", task.state];
    if (task.speculative_risk) classes.push("speculative");
    if (reducePhase === "shuffle") classes.push("phase-shuffle");
    if (reducePhase === "compute") classes.push("phase-compute");

    const details = [];
    if (task.type === "reduce" && reducePhase === "shuffle") details.push("Shuffle 收集中");
    else if (task.type === "reduce" && reducePhase === "compute") details.push("Reduce 计算");
    if (task.worker_id) details.push(task.worker_id);
    if (task.elapsed_ms) details.push(formatMs(task.elapsed_ms));
    if (task.attempt_id > 0) details.push(`a${task.attempt_id}`);
    if (task.retry_count > 0) details.push(`r${task.retry_count}`);

    const stateLabel =
      task.type === "reduce" && reducePhase === "shuffle"
        ? "shuffle"
        : task.type === "reduce" && reducePhase === "compute"
          ? "reduce"
          : task.state.replace("_", " ");
    const riskIcon = task.speculative_risk ? " ⚡" : "";

    return `<div class="${classes.join(" ")}" title="${escapeHtml(task.input_file || "")}">
      <span class="task-id">${task.type}-${task.id}${riskIcon}</span>
      <span class="task-state">${stateLabel}</span>
      <span class="task-detail">${escapeHtml(details.join(" · ") || "—")}</span>
    </div>`;
  }

  function renderTaskGrid(el, tasks, phaseMap) {
    const maxVisible = 240;
    const list = tasks || [];
    const visible = list.slice(0, maxVisible);
    const overflow = list.length - visible.length;
    const overflowCell = overflow > 0
      ? `<div class="task-overflow">
          <span class="task-id">+${overflow}</span>
          <span class="task-state">hidden</span>
          <span class="task-detail">大文件任务已汇总，避免前端渲染阻塞</span>
        </div>`
      : "";
    el.innerHTML =
      visible.map((task) => renderTaskCell(task, phaseMap?.get(task.id)?.phase)).join("") +
      overflowCell;
  }

  function renderReduceShuffleCard(info) {
    const ratio = pct(info.mapsReady, info.mapsTotal);
    const cls = ["reduce-shuffle-cell", info.phase];
    return `<article class="${cls.join(" ")}">
      <div class="reduce-shuffle-top">
        <span class="reduce-shuffle-id">R${info.rid}</span>
        <span class="reduce-shuffle-phase">${info.phase === "shuffle" ? "Shuffle 收集中" : "Reduce 计算"}</span>
      </div>
      <div class="reduce-shuffle-bar"><div class="reduce-shuffle-fill" style="width:${ratio}%"></div></div>
      <div class="reduce-shuffle-meta">
        ${info.mapsReady}/${info.mapsTotal} 中间文件
        ${info.task.worker_id ? ` · ${escapeHtml(info.task.worker_id)}` : ""}
        ${info.task.elapsed_ms ? ` · ${formatMs(info.task.elapsed_ms)}` : ""}
      </div>
    </article>`;
  }

  function renderReduceShuffleTrack(reduceTasks, partitions, unlocked, running) {
    const { shuffling, computing } = analyzeReducePhases(reduceTasks, partitions);
    const active = [...shuffling, ...computing];
    const hasCards = active.length > 0;

    if (els.reduceShuffleCount) {
      els.reduceShuffleCount.textContent = String(active.length);
    }
    if (els.reduceShuffleEmpty) {
      els.reduceShuffleEmpty.classList.toggle("hidden", hasCards);
      if (!hasCards) {
        if (unlocked && running) {
          els.reduceShuffleEmpty.textContent =
            "Slow Start 已解锁，等待 Worker 领取 Reduce 并开始 Shuffle…";
        } else if (unlocked) {
          els.reduceShuffleEmpty.textContent = "Slow Start 已解锁，当前无进行中的 Reduce Shuffle";
        } else {
          els.reduceShuffleEmpty.textContent =
            "尚未解锁 — 等待 Map 完成率到达 Slow Start 阈值";
        }
      }
    }
    if (els.reduceShuffleGrid) {
      els.reduceShuffleGrid.innerHTML = active.map(renderReduceShuffleCard).join("");
    }

    if (!els.reduceShuffleSummary) return { shuffling, computing, active };

    if (shuffling.length > 0) {
      els.reduceShuffleSummary.textContent = `${shuffling.length} 个收集中`;
    } else if (computing.length > 0) {
      els.reduceShuffleSummary.textContent = `${computing.length} 个计算中`;
    } else if (unlocked && running) {
      els.reduceShuffleSummary.textContent = "已解锁 · 等待调度";
    } else if (unlocked) {
      els.reduceShuffleSummary.textContent = "已解锁";
    } else {
      els.reduceShuffleSummary.textContent = "未解锁";
    }

    return { shuffling, computing, active };
  }

  function workerAssignmentIndex(mapTasks, reduceTasks) {
    const index = new Map();
    for (const task of [...(mapTasks || []), ...(reduceTasks || [])]) {
      if (task.state !== "in_progress" || !task.worker_id) continue;
      index.set(task.worker_id, { type: task.type || "task", id: task.id });
    }
    return index;
  }

  function workerCurrentAssignment(w, assignmentIndex) {
    const fromTask = assignmentIndex?.get(w.id);
    if (fromTask) return fromTask;
    if (Number(w.current_task) >= 0) {
      return { type: w.current_type || "task", id: w.current_task };
    }
    return null;
  }

  function formatWorkerTaskType(type) {
    if (type === "map") return "Map";
    if (type === "reduce") return "Reduce";
    return type;
  }

  function formatWorkerAssignmentLabel(assignment, reducePhases) {
    if (!assignment) return null;
    if (assignment.type === "reduce") {
      const phase = reducePhases?.phases?.get(assignment.id)?.phase;
      if (phase === "shuffle") return `Reduce R${assignment.id} · Shuffle`;
      if (phase === "compute") return `Reduce R${assignment.id} · 计算`;
    }
    return `${formatWorkerTaskType(assignment.type)} #${assignment.id}`;
  }

  function renderWorker(w, assignmentIndex, reducePhases) {
    const assignment = workerCurrentAssignment(w, assignmentIndex);
    const cls = ["worker-card"];
    if (w.blacklisted) cls.push("blacklisted");
    else if (assignment) cls.push("busy");
    else if (w.alive) cls.push("alive");
    if (!w.alive && !w.blacklisted) cls.push("dead");

    const badges = [];
    if (w.blacklisted) badges.push('<span class="badge blacklist">拉黑</span>');
    else if (assignment) badges.push('<span class="badge busy">执行中</span>');
    else if (w.alive) badges.push('<span class="badge alive">在线</span>');
    else badges.push('<span class="badge dead">离线</span>');

    const taskLine = assignment
      ? `<strong>${escapeHtml(formatWorkerAssignmentLabel(assignment, reducePhases) || "")}</strong>`
      : "空闲";

    return `<article class="${cls.join(" ")}">
      <div class="worker-top">
        <span class="worker-id">${escapeHtml(w.id)}</span>
        <div class="worker-badges">${badges.join("")}</div>
      </div>
      <div class="worker-meta">
        当前：${taskLine}<br/>
        失败：${w.failure_count} 次 · 心跳：${formatTime(w.last_heartbeat)}
      </div>
    </article>`;
  }

  function renderPartition(p) {
    const ratio = pct(p.maps_ready, p.maps_total);
    return `<div class="partition-cell ${p.ready ? "ready" : ""}">
      <div class="partition-title">R${p.reduce_id}</div>
      <div class="partition-bar"><div class="partition-fill" style="width:${ratio}%"></div></div>
      <div class="partition-meta">${p.maps_ready}/${p.maps_total}${p.ready ? " · 就绪" : ""}</div>
    </div>`;
  }

  function renderDecision(d) {
    const t = d.time ? formatTime(d.time) : "—";
    const type = (d.type || "unknown").replace(/\./g, "_");
    const isBanner = d.type === "job_completed" || d.type === "job_failed";
    if (isBanner) {
      return `<div class="decision-row decision-banner decision-type-${type}">
        <span class="decision-time">${t}</span>
        <span class="decision-type ${type}">${escapeHtml(d.type || "")}</span>
        <span class="decision-msg">${escapeHtml(d.message || "")}</span>
      </div>`;
    }
    return `<div class="decision-row decision-type-${type}">
      <span class="decision-time">${t}</span>
      <span class="decision-type ${type}">${escapeHtml(d.type || "")}</span>
      <span class="decision-msg">${escapeHtml(d.message || "")}</span>
    </div>`;
  }

  function decisionFingerprint(list) {
    return list
      .map((d) => `${d.time}|${d.type}|${d.message}|${d.task_id}|${d.attempt_id}`)
      .join("\n");
  }

  function decisionsForDisplay(decisions, job) {
    const list = [...(decisions || [])];
    if (job.state === "completed" && !list.some((d) => d.type === "job_completed")) {
      list.push({
        type: "job_completed",
        time: job.completed_at || null,
        message: `════ 作业完成 ════ Job ${job.id}`,
      });
    } else if (job.state === "failed" && !list.some((d) => d.type === "job_failed") && job.error) {
      list.push({
        type: "job_failed",
        time: job.completed_at || null,
        message: job.error,
      });
    }
    return list;
  }

  function renderDecisionsLog(decisions, job) {
    const logEl = els.decisionsLog;
    if (!logEl) return;

    const list = decisionsForDisplay(decisions, job);
    const fp = decisionFingerprint(list);
    const jobSwitched = job.id !== decisionsJobId;

    if (jobSwitched) {
      decisionsJobId = job.id;
      decisionsFingerprint = "";
    }

    if (fp === decisionsFingerprint) {
      return;
    }

    const prevScrollTop = logEl.scrollTop;
    const prevHeight = logEl.scrollHeight;
    const clientHeight = logEl.clientHeight;
    const wasPinnedBottom = prevHeight - clientHeight - prevScrollTop < 48;
    const isRunning = job.state === "running";

    decisionsFingerprint = fp;

    const hasDecisions = list.length > 0;
    if (els.decisionsCard) {
      els.decisionsCard.classList.toggle("card--log-filled", hasDecisions);
    }
    if (els.decisionsEmpty) {
      els.decisionsEmpty.classList.toggle("hidden", hasDecisions);
    }
    if (els.decisionsCount) {
      els.decisionsCount.textContent = String(list.length);
    }

    logEl.innerHTML = list.map(renderDecision).join("");

    if (jobSwitched) {
      logEl.scrollTop = logEl.scrollHeight;
    } else if (!paused && isRunning && wasPinnedBottom) {
      logEl.scrollTop = logEl.scrollHeight;
    } else if (prevHeight > 0) {
      logEl.scrollTop = prevScrollTop + (logEl.scrollHeight - prevHeight);
    }
  }

  function updatePipeline(job, prog, reduceShuffle) {
    const mapPct = pct(prog.map_completed, prog.map_total);
    const reducePct = pct(prog.reduce_completed, prog.reduce_total);
    const unlocked = prog.reduce_scheduling_unlocked;
    const running = job.state === "running";
    const shuffling = reduceShuffle?.shuffling?.length ?? 0;
    const computing = reduceShuffle?.computing?.length ?? 0;

    els.pipeMapPct.textContent = `${mapPct}%`;
    els.pipeMapBar.style.width = `${mapPct}%`;
    els.pipeReducePct.textContent = `${reducePct}%`;
    els.pipeReduceBar.style.width = `${reducePct}%`;

    const mapNode = els.pipeline?.querySelector(".pipe-node--map");
    const shuffleNode = els.pipeline?.querySelector(".pipe-node--shuffle");
    const reduceNode = els.pipeline?.querySelector(".pipe-node--reduce");

    mapNode?.classList.toggle("active", running && mapPct < 100);
    shuffleNode?.classList.toggle("active", running && shuffling > 0);
    shuffleNode?.classList.toggle("unlocked", unlocked && shuffling === 0);
    reduceNode?.classList.toggle("active", running && (computing > 0 || reducePct > 0));

    if (shuffling > 0) {
      els.pipeShuffleLabel.textContent = `${shuffling} 收集中`;
      if (els.pipeShuffleHint) els.pipeShuffleHint.textContent = "轮询中间文件";
    } else if (unlocked && running) {
      els.pipeShuffleLabel.textContent = computing > 0 ? "收齐" : "已解锁";
      if (els.pipeShuffleHint) {
        els.pipeShuffleHint.textContent = computing > 0 ? "等待下一批" : "可调度 Reduce";
      }
    } else if (unlocked) {
      els.pipeShuffleLabel.textContent = "已解锁";
      if (els.pipeShuffleHint) els.pipeShuffleHint.textContent = "慢启动";
    } else {
      const threshold = Math.round((prog.reduce_slow_start_threshold ?? 0.8) * 100);
      const mapRatio = Math.round((prog.map_ratio ?? 0) * 100);
      els.pipeShuffleLabel.textContent = `${mapRatio}%/${threshold}%`;
      if (els.pipeShuffleHint) els.pipeShuffleHint.textContent = "等待阈值";
    }

    els.pipeShuffleConn?.classList.toggle("flowing", running && shuffling > 0 && mapPct < 100);
    els.pipeReduceConn?.classList.toggle("flowing", running && (computing > 0 || reducePct > 0));
  }

  function renderHistoryItem(job) {
    const mapPct = progressRatio(job.map_completed, job.map_total);
    const reducePct = progressRatio(job.reduce_completed, job.reduce_total);
    const classes = ["job-history-item", job.state || "unknown"];
    if (job.is_selected) classes.push("selected");
    if (job.is_current) classes.push("current");
    const input = (job.input_files || []).map((p) => p.split(/[\\/]/).pop()).join(", ") || "—";
    const time = job.completed_at || job.created_at;
    return `<button class="${classes.join(" ")}" type="button" data-job-id="${escapeHtml(job.id)}" title="${escapeHtml(job.id)}">
      <span class="history-id">${escapeHtml(shortJobID(job.id))}</span>
      <span class="history-state">${escapeHtml(job.state || "unknown")}${job.is_current ? " · current" : ""}</span>
      <span class="history-input">${escapeHtml(input)}</span>
      <span class="history-progress">
        <span>Map ${job.map_completed ?? 0}/${job.map_total ?? 0}</span>
        <span>Reduce ${job.reduce_completed ?? 0}/${job.reduce_total ?? 0}</span>
      </span>
      <span class="history-bars" aria-hidden="true">
        <i style="width:${mapPct}%"></i>
        <b style="width:${reducePct}%"></b>
      </span>
      <span class="history-time">${formatDateTime(time)}</span>
    </button>`;
  }

  function renderJobHistory(history) {
    const list = history || [];
    if (els.historyCount) {
      els.historyCount.textContent = String(list.length);
    }
    if (!els.jobHistory || !els.jobHistoryEmpty) return;
    els.jobHistoryEmpty.classList.toggle("hidden", list.length > 0);
    els.jobHistory.innerHTML = list.map(renderHistoryItem).join("");
  }

  function renderOptimizations(data, prog) {
    const opt = data.optimizations || {};
    const slow = opt.slow_start || {};
    const shuffle = opt.shuffle || {};
    const streaming = opt.streaming || {};
    const fault = opt.fault_tolerance || {};

    const mapRatio = Math.round((slow.map_ratio ?? prog.map_ratio ?? 0) * 100);
    const threshold = Math.round((slow.threshold ?? prog.reduce_slow_start_threshold ?? 0.8) * 100);
    const early = Number(slow.early_reduce_starts || 0);
    els.optSlowStatus.textContent = early > 0 ? "已提前" : slow.unlocked ? "已解锁" : "等待";
    els.optSlowMetric.textContent = `${mapRatio}% / ${threshold}%`;
    els.optSlowBar.style.width = `${Math.min(100, mapRatio)}%`;
    els.optSlowDetail.textContent = early > 0
      ? `提前启动 ${early} 个 Reduce · 活跃 ${slow.active_reduces || 0}`
      : `阈值 ${threshold}% · ${slow.enabled ? "慢启动开启" : "全量 Map 后启动"}`;

    const saved = Number(shuffle.compressed_saved_percent || 0);
    els.optShuffleSaved.textContent = shuffle.json_bytes ? `节省 ${formatPercent(saved)}` : "收集中";
    els.optShuffleRecords.textContent = `${formatCount(shuffle.records)} rec`;
    els.optShuffleBefore.textContent = `JSONL ${formatBytes(shuffle.json_bytes)}`;
    els.optShuffleAfter.textContent = `gzip ${formatBytes(shuffle.compressed_bytes)}`;
    els.optShuffleBar.style.width = `${Math.max(0, Math.min(100, saved))}%`;

    els.optStreamRecords.textContent = `${formatCount(streaming.streamed_records)} rec`;
    els.optStreamBuffer.textContent = `buffer ${formatCount(streaming.max_buffered_values)}`;
    els.optStreamDetail.textContent = `${formatCount(streaming.output_keys)} keys · ${formatCount(streaming.opened_streams)} streams · ${formatMs(streaming.stream_ms || 0)} merge`;

    const issueCount =
      Number(fault.task_failures || 0) +
      Number(fault.task_timeouts || 0) +
      Number(fault.worker_timeouts || 0) +
      Number(fault.blacklisted_workers || 0) +
      Number(fault.stale_reports || 0);
    els.optFaultStatus.textContent = issueCount > 0 ? "已介入" : (fault.workers_total > 0 ? "健康" : "待心跳");
    els.optFaultChecks.textContent = `${formatCount(fault.workers_alive || 0)} / ${formatCount(fault.workers_total || 0)}`;
    els.optFaultDetail.textContent = `重试 ${fault.retries || 0} · 超时 ${fault.task_timeouts || 0} · 黑名单 ${fault.blacklisted_workers || 0} · 陈旧上报 ${fault.stale_reports || 0}`;
  }

  function render(data) {
    renderJobHistory(data.job_history || []);

    if (!data.job || !data.job.id) {
      els.emptyState.classList.remove("hidden");
      els.dashboard.classList.add("hidden");
      els.pollStatus.textContent = "已连接 · 无作业";
      setLiveState("live");
      return;
    }

    els.emptyState.classList.add("hidden");
    els.dashboard.classList.remove("hidden");

    const job = data.job;
    const prog = data.progress || {};

    els.heroJobLabel.textContent = stateLabels[job.state] || "作业状态";
    els.jobState.textContent = job.state;
    els.jobState.className = `state-pill ${job.state}`;
    els.jobIdDisplay.textContent = `Job: ${job.id}`;

    els.mapProgressText.textContent = `${prog.map_completed ?? 0} / ${prog.map_total ?? 0}`;
    els.reduceProgressText.textContent = `${prog.reduce_completed ?? 0} / ${prog.reduce_total ?? 0}`;

    const unlocked = prog.reduce_scheduling_unlocked;
    els.slowstartStatus.textContent = unlocked ? "已解锁" : "锁定";
    els.slowstartStatus.className = `slowstart-badge ${unlocked ? "unlocked" : "locked"}`;
    const threshold = Math.round((prog.reduce_slow_start_threshold ?? 0.8) * 100);
    const mapRatio = Math.round((prog.map_ratio ?? 0) * 100);
    els.slowstartDetail.textContent = `Map ${mapRatio}% · 阈值 ${threshold}%`;

    const reducePhases = analyzeReducePhases(data.reduce_tasks, data.reduce_partitions);
    const reduceShuffle = renderReduceShuffleTrack(
      data.reduce_tasks,
      data.reduce_partitions,
      prog.reduce_scheduling_unlocked,
      job.state === "running"
    );
    updatePipeline(job, prog, reduceShuffle);
    renderOptimizations(data, prog);

    const cfg = job.config || {};
    renderConfig(els.jobConfig, [
      ["输入", (cfg.input_files || []).join(", ")],
      ["Map", cfg.n_map],
      ["Reduce", cfg.n_reduce],
      ["Map 函数", cfg.map_func || "—"],
      ["Reduce", cfg.reduce_func || "—"],
      ["Combine", cfg.combine_func || "—"],
      ["分片大小", cfg.split_size ? formatBytes(cfg.split_size) : "32 MB（默认）"],
      ["目录", cfg.work_dir],
      ["Slow Start", cfg.reduce_slow_start],
    ]);

    const sched = data.scheduling || {};
    renderConfig(els.schedulingInsight, [
      ["Map 中位", sched.map_median_ms != null ? `${sched.map_median_ms} ms` : "—"],
      ["Reduce 中位", sched.reduce_median_ms != null ? `${sched.reduce_median_ms} ms` : "—"],
      ["倍数", sched.speculative_multiplier],
      ["最少样本", sched.speculative_min_completed],
      ["Map 样本", sched.map_completed_samples],
      ["Reduce 样本", sched.reduce_completed_samples],
      ["推测阈值", sched.map_median_ms
        ? `${Math.round(sched.map_median_ms * (sched.speculative_multiplier || 1.5))} ms`
        : "收集中"],
    ]);

    const workers = data.workers || [];
    const workerAssignments = workerAssignmentIndex(data.map_tasks, data.reduce_tasks);
    els.workerCount.textContent = String(workers.length);
    els.workersEmpty.classList.toggle("hidden", workers.length > 0);
    els.workersGrid.innerHTML = workers
      .map((w) => renderWorker(w, workerAssignments, reducePhases))
      .join("");

    renderTaskGrid(els.mapTasks, data.map_tasks || []);
    renderTaskGrid(els.reduceTasks, data.reduce_tasks || [], reducePhases.phases);
    els.partitionGrid.innerHTML = (data.reduce_partitions || []).map(renderPartition).join("");

    const decisions = data.decisions || [];
    renderDecisionsLog(decisions, job);

    els.lastUpdate.textContent = `更新 ${formatDateTime(data.server_time)}`;
    if (!paused) {
      els.pollStatus.textContent = "实时";
      setLiveState("live");
    }
  }

  async function fetchDashboard() {
    const job = els.jobId.value.trim();
    const q = job ? `?job=${encodeURIComponent(job)}` : "";
    const res = await fetch(`${apiBase()}/api/dashboard${q}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  async function tick() {
    if (paused) return;
    try {
      const data = await fetchDashboard();
      render(data);
    } catch (err) {
      els.pollStatus.textContent = "连接失败";
      setLiveState("error");
    }
  }

  function startPolling() {
    if (polling) return;
    polling = true;
    paused = false;
    els.btnPause.textContent = "暂停";
    els.btnPause.setAttribute("aria-pressed", "false");
    tick();
    timer = setInterval(tick, POLL_MS);
  }

  function stopPolling() {
    polling = false;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }

  els.btnConnect.addEventListener("click", () => {
    stopPolling();
    startPolling();
  });

  els.btnLatest?.addEventListener("click", () => {
    els.jobId.value = "";
    stopPolling();
    startPolling();
  });

  els.jobHistory?.addEventListener("click", (event) => {
    const item = event.target.closest("[data-job-id]");
    if (!item) return;
    els.jobId.value = item.dataset.jobId || "";
    stopPolling();
    startPolling();
  });

  els.btnTheme.addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
    document.documentElement.dataset.theme = next;
    try {
      localStorage.setItem("minimr-theme", next);
    } catch (e) {
      /* 隐私模式下 localStorage 不可用，主题仅本次生效 */
    }
  });

  els.btnPause.addEventListener("click", () => {
    paused = !paused;
    els.btnPause.textContent = paused ? "继续" : "暂停";
    els.btnPause.setAttribute("aria-pressed", String(paused));
    if (paused) {
      els.pollStatus.textContent = "已暂停";
      setLiveState("paused");
    } else {
      tick();
    }
  });

  if (window.location.port === "8081" || window.location.pathname === "/") {
    startPolling();
  }
})();

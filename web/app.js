(function () {
  "use strict";

  const POLL_MS = 1500;

  const $ = (id) => document.getElementById(id);

  const els = {
    masterUrl: $("master-url"),
    jobId: $("job-id"),
    btnConnect: $("btn-connect"),
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
    pipeShuffleConn: $("pipe-shuffle-conn"),
    pipeReducePct: $("pipe-reduce-pct"),
    pipeReduceBar: $("pipe-reduce-bar"),
    pipeReduceConn: $("pipe-reduce-conn"),
    mapProgressText: $("map-progress-text"),
    reduceProgressText: $("reduce-progress-text"),
    slowstartStatus: $("slowstart-status"),
    slowstartDetail: $("slowstart-detail"),
    jobConfig: $("job-config"),
    schedulingInsight: $("scheduling-insight"),
    workersGrid: $("workers-grid"),
    workersEmpty: $("workers-empty"),
    workerCount: $("worker-count"),
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
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
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

  function renderTaskCell(task) {
    const classes = ["task-cell", task.state];
    if (task.speculative_risk) classes.push("speculative");
    const details = [];
    if (task.worker_id) details.push(task.worker_id);
    if (task.elapsed_ms) details.push(formatMs(task.elapsed_ms));
    if (task.attempt_id > 0) details.push(`a${task.attempt_id}`);
    if (task.retry_count > 0) details.push(`r${task.retry_count}`);
    const riskIcon = task.speculative_risk ? ' ⚡' : '';
    return `<div class="${classes.join(" ")}" title="${escapeHtml(task.input_file || "")}">
      <span class="task-id">${task.type}-${task.id}${riskIcon}</span>
      <span class="task-state">${task.state.replace("_", " ")}</span>
      <span class="task-detail">${escapeHtml(details.join(" · ") || "—")}</span>
    </div>`;
  }

  function renderWorker(w) {
    const cls = ["worker-card"];
    if (w.blacklisted) cls.push("blacklisted");
    else if (w.alive) cls.push("alive");
    if (!w.alive && !w.blacklisted) cls.push("dead");

    const badges = [];
    if (w.blacklisted) badges.push('<span class="badge blacklist">拉黑</span>');
    else if (w.alive) badges.push('<span class="badge alive">在线</span>');
    else badges.push('<span class="badge dead">离线</span>');

    const taskLine =
      w.current_task >= 0
        ? `<strong>${w.current_type || "task"} #${w.current_task}</strong>`
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
    return `<div class="decision-row decision-type-${type}">
      <span class="decision-time">${t}</span>
      <span class="decision-type ${type}">${escapeHtml(d.type || "")}</span>
      <span class="decision-msg">${escapeHtml(d.message || "")}</span>
    </div>`;
  }

  function updatePipeline(job, prog) {
    const mapPct = pct(prog.map_completed, prog.map_total);
    const reducePct = pct(prog.reduce_completed, prog.reduce_total);
    const unlocked = prog.reduce_scheduling_unlocked;
    const running = job.state === "running";

    els.pipeMapPct.textContent = `${mapPct}%`;
    els.pipeMapBar.style.width = `${mapPct}%`;
    els.pipeReducePct.textContent = `${reducePct}%`;
    els.pipeReduceBar.style.width = `${reducePct}%`;

    const mapNode = els.pipeline?.querySelector(".pipe-node--map");
    const shuffleNode = els.pipeline?.querySelector(".pipe-node--shuffle");
    const reduceNode = els.pipeline?.querySelector(".pipe-node--reduce");

    mapNode?.classList.toggle("active", running && mapPct < 100);
    shuffleNode?.classList.toggle("active", running && unlocked);
    reduceNode?.classList.toggle("active", running && (reducePct > 0 || unlocked));

    if (unlocked) {
      els.pipeShuffleLabel.textContent = running ? "进行中" : "已解锁";
    } else {
      const threshold = Math.round((prog.reduce_slow_start_threshold ?? 0.8) * 100);
      const mapRatio = Math.round((prog.map_ratio ?? 0) * 100);
      els.pipeShuffleLabel.textContent = `${mapRatio}%/${threshold}%`;
    }

    els.pipeShuffleConn?.classList.toggle("flowing", running && unlocked && mapPct < 100);
    els.pipeReduceConn?.classList.toggle("flowing", running && unlocked && reducePct < 100);
  }

  function render(data) {
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

    prog.reduce_partitions_ready = (data.reduce_partitions || []).filter((p) => p.ready).length;
    updatePipeline(job, prog);

    const cfg = job.config || {};
    renderConfig(els.jobConfig, [
      ["输入", (cfg.input_files || []).join(", ")],
      ["Map", cfg.n_map],
      ["Reduce", cfg.n_reduce],
      ["Map 函数", cfg.map_func || "—"],
      ["Reduce", cfg.reduce_func || "—"],
      ["Combine", cfg.combine_func || "—"],
      ["分片", cfg.split_size ? `${cfg.split_size} B` : "64 KB"],
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
    els.workerCount.textContent = String(workers.length);
    els.workersEmpty.classList.toggle("hidden", workers.length > 0);
    els.workersGrid.innerHTML = workers.map(renderWorker).join("");

    els.mapTasks.innerHTML = (data.map_tasks || []).map(renderTaskCell).join("");
    els.reduceTasks.innerHTML = (data.reduce_tasks || []).map(renderTaskCell).join("");
    els.partitionGrid.innerHTML = (data.reduce_partitions || []).map(renderPartition).join("");

    const decisions = [...(data.decisions || [])].reverse();
    const hasDecisions = decisions.length > 0;
    if (els.decisionsCard) {
      els.decisionsCard.classList.toggle("card--log-filled", hasDecisions);
    }
    if (els.decisionsCount) {
      els.decisionsCount.textContent = String(decisions.length);
    }
    els.decisionsLog.innerHTML = decisions.map(renderDecision).join("");

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

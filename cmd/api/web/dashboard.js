(function () {
  const queueCardsEl = document.getElementById("queue-cards");
  const queueSummaryEl = document.getElementById("queue-summary");
  const queueCountEl = document.getElementById("queue-count");
  const selectedQueueTitleEl = document.getElementById("selected-queue-title");
  const selectedQueueStatusEl = document.getElementById("selected-queue-status");
  const queueDetailStatsEl = document.getElementById("queue-detail-stats");
  const lastUpdatedEl = document.getElementById("last-updated");
  const refreshButtonEl = document.getElementById("refresh-button");

  const taskTargets = {
    pending: document.getElementById("pending-list"),
    active: document.getElementById("active-list"),
    scheduled: document.getElementById("scheduled-list"),
    retry: document.getElementById("retry-list"),
    archived: document.getElementById("archived-list"),
  };

  const taskCountTargets = {
    pending: document.getElementById("pending-count"),
    active: document.getElementById("active-count"),
    scheduled: document.getElementById("scheduled-count"),
    retry: document.getElementById("retry-count"),
    archived: document.getElementById("archived-count"),
  };

  let selectedQueue = "";

  async function fetchJSON(path) {
    const response = await fetch(path);
    if (!response.ok) {
      throw new Error("Request failed: " + response.status);
    }
    return response.json();
  }

  async function loadDashboard() {
    refreshButtonEl.disabled = true;

    try {
      const queueResponse = await fetchJSON("/api/v1/queues");
      const queues = queueResponse.queues || [];
      queueCountEl.textContent = queues.length + " queue" + (queues.length === 1 ? "" : "s");

      if (!selectedQueue && queues.length > 0) {
        selectedQueue = queues[0];
      }
      if (queues.length > 0 && !queues.includes(selectedQueue)) {
        selectedQueue = queues[0];
      }

      const stats = await Promise.all(
        queues.map(async function (queueName) {
          const detail = await fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/stats");
          return detail;
        })
      );

      renderSummary(stats);
      renderQueueCards(stats);

      if (selectedQueue) {
        await loadQueueDetail(selectedQueue);
      } else {
        resetDetail();
      }

      lastUpdatedEl.textContent = "Updated " + new Date().toLocaleTimeString();
    } catch (error) {
      queueCardsEl.innerHTML = '<div class="empty-state">Failed to load queue data. ' + error.message + "</div>";
      queueSummaryEl.innerHTML = "";
      resetDetail();
      lastUpdatedEl.textContent = "Last refresh failed";
    } finally {
      refreshButtonEl.disabled = false;
    }
  }

  async function loadQueueDetail(queueName) {
    selectedQueue = queueName;
    selectedQueueTitleEl.textContent = queueName + " queue";

    const [stats, pending, active, scheduled, retry, archived] = await Promise.all([
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/stats"),
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/pending?page=1&page_size=10"),
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/active?page=1&page_size=10"),
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/scheduled?page=1&page_size=10"),
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/retry?page=1&page_size=10"),
      fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/archived?page=1&page_size=10"),
    ]);

    renderQueueCardsSelection();
    renderDetailStats(stats);
    renderTaskList("pending", pending.tasks || []);
    renderTaskList("active", active.tasks || []);
    renderTaskList("scheduled", scheduled.tasks || []);
    renderTaskList("retry", retry.tasks || []);
    renderTaskList("archived", archived.tasks || []);

    if (stats.archived > 0) {
      selectedQueueStatusEl.textContent = "Archived tasks present";
      selectedQueueStatusEl.className = "pill";
    } else if (stats.retry > 0) {
      selectedQueueStatusEl.textContent = "Retries pending";
      selectedQueueStatusEl.className = "pill";
    } else if (stats.paused) {
      selectedQueueStatusEl.textContent = "Paused";
      selectedQueueStatusEl.className = "pill muted";
    } else {
      selectedQueueStatusEl.textContent = "Healthy";
      selectedQueueStatusEl.className = "pill";
    }
  }

  function renderSummary(stats) {
    const totals = stats.reduce(
      function (acc, queue) {
        acc.size += queue.size || 0;
        acc.active += queue.active || 0;
        acc.pending += queue.pending || 0;
        acc.scheduled += queue.scheduled || 0;
        acc.retry += queue.retry || 0;
        acc.archived += queue.archived || 0;
        return acc;
      },
      { size: 0, active: 0, pending: 0, scheduled: 0, retry: 0, archived: 0 }
    );

    queueSummaryEl.innerHTML = [
      summaryCard("Queued now", totals.size),
      summaryCard("Active", totals.active),
      summaryCard("Pending", totals.pending),
      summaryCard("Scheduled", totals.scheduled),
      summaryCard("Retry", totals.retry),
      summaryCard("Archived", totals.archived),
    ].join("");
  }

  function summaryCard(label, value) {
    return '<div class="summary-card"><span>' + label + "</span><strong>" + value + "</strong></div>";
  }

  function renderQueueCards(stats) {
    if (stats.length === 0) {
      queueCardsEl.innerHTML = '<div class="empty-state">No queues found yet. Enqueue a task to populate the dashboard.</div>';
      return;
    }

    const maxSize = Math.max.apply(null, stats.map(function (queue) { return queue.size || 0; }), 1);

    queueCardsEl.innerHTML = stats
      .map(function (queue) {
        const fill = Math.max(((queue.size || 0) / maxSize) * 100, queue.size > 0 ? 12 : 0);
        return [
          '<button class="queue-card' + (queue.queue === selectedQueue ? " is-active" : "") + '" data-queue="' + escapeHTML(queue.queue) + '">',
          '<div class="queue-card-header"><strong>' + escapeHTML(queue.queue) + '</strong><span>' + (queue.size || 0) + " total</span></div>",
          '<div class="queue-card-meta"><span>Pending ' + (queue.pending || 0) + '</span><span>Active ' + (queue.active || 0) + "</span></div>",
          '<div class="queue-card-meta"><span>Retry ' + (queue.retry || 0) + '</span><span>Archived ' + (queue.archived || 0) + "</span></div>",
          '<div class="queue-bar"><div class="queue-bar-fill" style="width:' + fill + '%"></div></div>',
          "</button>",
        ].join("");
      })
      .join("");

    Array.prototype.forEach.call(document.querySelectorAll(".queue-card"), function (button) {
      button.addEventListener("click", function () {
        loadQueueDetail(button.dataset.queue).catch(function (error) {
          selectedQueueStatusEl.textContent = error.message;
        });
      });
    });
  }

  function renderQueueCardsSelection() {
    Array.prototype.forEach.call(document.querySelectorAll(".queue-card"), function (button) {
      button.classList.toggle("is-active", button.dataset.queue === selectedQueue);
    });
  }

  function renderDetailStats(stats) {
    queueDetailStatsEl.classList.remove("empty-state");
    queueDetailStatsEl.innerHTML = [
      detailCard("Total", stats.size),
      detailCard("Processed today", stats.processed_today),
      detailCard("Failed today", stats.failed_today),
      detailCard("Paused", stats.paused ? "Yes" : "No"),
    ].join("");
  }

  function detailCard(label, value) {
    return '<div class="detail-card"><span>' + label + "</span><strong>" + value + "</strong></div>";
  }

  function renderTaskList(kind, tasks) {
    taskCountTargets[kind].textContent = String(tasks.length);

    if (tasks.length === 0) {
      taskTargets[kind].className = "task-list empty-state";
      taskTargets[kind].textContent = "No " + kind + " tasks right now.";
      return;
    }

    taskTargets[kind].className = "task-list";
    taskTargets[kind].innerHTML = tasks.map(renderTaskRow).join("");
  }

  function renderTaskRow(task) {
    return [
      '<article class="task-row">',
      '<div class="task-row-header"><span class="task-type">' + escapeHTML(task.type || "unknown") + '</span><span>' + escapeHTML(task.state || "") + "</span></div>",
      '<div class="task-row-meta"><span class="task-id">' + escapeHTML(task.id || "n/a") + '</span><span>Retries ' + String(task.retry_count || 0) + "</span></div>",
      '<pre class="task-payload">' + escapeHTML(formatPayload(task.payload)) + "</pre>",
      "</article>",
    ].join("");
  }

  function formatPayload(payload) {
    if (!payload) {
      return "No payload";
    }
    if (typeof payload === "string") {
      return payload;
    }
    try {
      return JSON.stringify(payload, null, 2);
    } catch (error) {
      return String(payload);
    }
  }

  function resetDetail() {
    selectedQueueTitleEl.textContent = "Select a queue";
    selectedQueueStatusEl.textContent = "Waiting";
    selectedQueueStatusEl.className = "pill muted";
    queueDetailStatsEl.className = "detail-stats empty-state";
    queueDetailStatsEl.textContent = "Choose a queue to inspect its current state and task lists.";

    Object.keys(taskTargets).forEach(function (kind) {
      taskCountTargets[kind].textContent = "0";
      taskTargets[kind].className = "task-list empty-state";
      taskTargets[kind].textContent = "No tasks loaded.";
    });
  }

  function escapeHTML(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  refreshButtonEl.addEventListener("click", function () {
    loadDashboard();
  });

  loadDashboard();
  window.setInterval(loadDashboard, 5000);
})();

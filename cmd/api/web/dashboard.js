(function () {
  const queueCardsEl = document.getElementById("queue-cards");
  const queueSummaryEl = document.getElementById("queue-summary");
  const queueCountEl = document.getElementById("queue-count");
  const selectedQueueTitleEl = document.getElementById("selected-queue-title");
  const selectedQueueStatusEl = document.getElementById("selected-queue-status");
  const queueDetailStatsEl = document.getElementById("queue-detail-stats");
  const lastUpdatedEl = document.getElementById("last-updated");
  const refreshButtonEl = document.getElementById("refresh-button");
  const feedbackBannerEl = document.getElementById("action-feedback");
  const pauseQueueButtonEl = document.getElementById("pause-queue-button");
  const unpauseQueueButtonEl = document.getElementById("unpause-queue-button");
  const emailFormEl = document.getElementById("email-form");
  const imageFormEl = document.getElementById("image-form");

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

  async function fetchJSON(path, options) {
    const response = await fetch(path, options);
    const isJSON = (response.headers.get("content-type") || "").includes("application/json");
    const body = isJSON ? await response.json() : null;

    if (!response.ok) {
      const errorMessage = body && body.error ? body.error : "Request failed: " + response.status;
      throw new Error(errorMessage);
    }

    return body;
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
          return fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/stats");
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
      queueCardsEl.innerHTML = '<div class="empty-state">Failed to load queue data. ' + escapeHTML(error.message) + "</div>";
      queueSummaryEl.innerHTML = "";
      resetDetail();
      showFeedback(error.message, true);
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

    pauseQueueButtonEl.disabled = !selectedQueue || stats.paused;
    unpauseQueueButtonEl.disabled = !selectedQueue || !stats.paused;

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

    const sizes = stats.map(function (queue) { return queue.size || 0; });
    const maxSize = Math.max(1, ...sizes);

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
          showFeedback(error.message, true);
          selectedQueueStatusEl.textContent = "Load failed";
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
    taskTargets[kind].innerHTML = tasks.map(function (task) {
      return renderTaskRow(kind, task);
    }).join("");

    bindTaskActionButtons(kind);
  }

  function renderTaskRow(kind, task) {
    return [
      '<article class="task-row">',
      '<div class="task-row-header"><span class="task-type">' + escapeHTML(task.type || "unknown") + '</span><span>' + escapeHTML(task.state || kind) + "</span></div>",
      '<div class="task-row-meta"><span class="task-id">' + escapeHTML(task.id || "n/a") + '</span><span>Retries ' + String(task.retry_count || 0) + "</span></div>",
      '<pre class="task-payload">' + escapeHTML(formatPayload(task.payload)) + "</pre>",
      renderTaskActions(kind, task),
      "</article>",
    ].join("");
  }

  function renderTaskActions(kind, task) {
    const taskID = escapeHTML(task.id || "");
    const queueName = escapeHTML(selectedQueue);
    const actions = [];

    if (kind === "scheduled" || kind === "retry" || kind === "archived") {
      actions.push('<button type="button" class="task-action" data-action="run" data-queue="' + queueName + '" data-task-id="' + taskID + '">Run now</button>');
    }
    actions.push('<button type="button" class="task-action" data-action="cancel" data-queue="' + queueName + '" data-task-id="' + taskID + '">Cancel job</button>');

    if (actions.length === 0) {
      return "";
    }

    return '<div class="task-actions">' + actions.join("") + "</div>";
  }

  function bindTaskActionButtons(kind) {
    Array.prototype.forEach.call(taskTargets[kind].querySelectorAll(".task-action"), function (button) {
      button.addEventListener("click", async function () {
        const action = button.dataset.action;
        const queueName = button.dataset.queue;
        const taskID = button.dataset.taskId;

        button.disabled = true;
        try {
          if (action === "run") {
            await fetchJSON("/api/v1/queues/" + encodeURIComponent(queueName) + "/tasks/" + encodeURIComponent(taskID) + "/run", {
              method: "POST",
            });
            showFeedback("Task " + taskID + " moved back to pending.");
          } else if (action === "cancel") {
            await fetchJSON("/api/v1/jobs/" + encodeURIComponent(taskID) + "/cancel", {method:"POST"});
            showFeedback("Job " + taskID + " canceled.");
          }

          await loadDashboard();
        } catch (error) {
          showFeedback(error.message, true);
        } finally {
          button.disabled = false;
        }
      });
    });
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
    pauseQueueButtonEl.disabled = true;
    unpauseQueueButtonEl.disabled = true;

    Object.keys(taskTargets).forEach(function (kind) {
      taskCountTargets[kind].textContent = "0";
      taskTargets[kind].className = "task-list empty-state";
      taskTargets[kind].textContent = "No tasks loaded.";
    });
  }

  function showFeedback(message, isError) {
    feedbackBannerEl.textContent = message;
    feedbackBannerEl.className = "feedback-banner" + (isError ? " is-error" : "");
  }

  function readFormData(formElement) {
    const formData = new FormData(formElement);
    const output = {};

    formData.forEach(function (value, key) {
      output[key] = value;
    });

    if (output.user_id !== undefined) {
      output.user_id = Number(output.user_id);
    }
    if (output.width !== undefined) {
      output.width = Number(output.width);
    }
    if (output.height !== undefined) {
      output.height = Number(output.height);
    }
    if (output.process_in_seconds !== undefined) {
      output.process_in_seconds = Number(output.process_in_seconds);
    }

    return output;
  }

  async function submitTaskForm(formElement, path, successLabel) {
    const submitButton = formElement.querySelector('button[type="submit"]');
    submitButton.disabled = true;

    try {
      const payload = readFormData(formElement);
      const result = await fetchJSON(path, {
        method: "POST",
        headers: {
          "Idempotency-Key": formElement.dataset.idempotencyKey || (formElement.dataset.idempotencyKey = crypto.randomUUID()),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      delete formElement.dataset.idempotencyKey;
      showFeedback(successLabel + " queued successfully. Task ID: " + escapeHTML(result.data.task_id));
      if (payload.queue) {
        selectedQueue = payload.queue;
      }
      await loadDashboard();
    } catch (error) {
      showFeedback(error.message, true);
    } finally {
      submitButton.disabled = false;
    }
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

  pauseQueueButtonEl.addEventListener("click", async function () {
    if (!selectedQueue) {
      return;
    }
    pauseQueueButtonEl.disabled = true;
    try {
      await fetchJSON("/api/v1/queues/" + encodeURIComponent(selectedQueue) + "/pause", { method: "POST" });
      showFeedback("Queue " + selectedQueue + " paused.");
      await loadDashboard();
    } catch (error) {
      showFeedback(error.message, true);
    }
  });

  unpauseQueueButtonEl.addEventListener("click", async function () {
    if (!selectedQueue) {
      return;
    }
    unpauseQueueButtonEl.disabled = true;
    try {
      await fetchJSON("/api/v1/queues/" + encodeURIComponent(selectedQueue) + "/unpause", { method: "POST" });
      showFeedback("Queue " + selectedQueue + " resumed.");
      await loadDashboard();
    } catch (error) {
      showFeedback(error.message, true);
    }
  });

  emailFormEl.addEventListener("submit", function (event) {
    event.preventDefault();
    submitTaskForm(emailFormEl, "/api/v1/tasks/email", "Email task");
  });

  imageFormEl.addEventListener("submit", function (event) {
    event.preventDefault();
    submitTaskForm(imageFormEl, "/api/v1/tasks/image/resize", "Image task");
  });

  [emailFormEl,imageFormEl].forEach(function(form) {form.addEventListener("input",function(){delete form.dataset.idempotencyKey;});});
  resetDetail();
  loadDashboard();
  window.setInterval(loadDashboard, 5000);
})();

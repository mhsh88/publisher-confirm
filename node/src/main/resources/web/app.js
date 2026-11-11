(() => {
  const nodesEl = document.getElementById("nodes");
  const feedEl = document.getElementById("feed");
  const nodeCountEl = document.getElementById("nodeCount");
  const statusEl = document.getElementById("status");
  const statusText = document.getElementById("statusText");
  const sendBtn = document.getElementById("send");

  let knownEventKeys = new Set();
  let lastSelf = "";
  let lastNodesKey = "";
  let lastWorkersKey = "";
  let sending = false;

  function shortNode(address) {
    if (!address) return "?";
    const host = address.split("@")[1] || address;
    return host.replace(":2551", "");
  }

  function eventKey(ev) {
    return `${ev.at}|${ev.kind}|${ev.correlationId}|${ev.workerId}|${ev.otherWorkerId}|${ev.body}`;
  }

  function flash(el, className) {
    if (!el) return;
    el.classList.remove(className);
    void el.offsetWidth;
    el.classList.add(className);
    setTimeout(() => el.classList.remove(className), 900);
  }

  function renderNodes(snapshot) {
    const nodesKey = (snapshot.nodes || []).join("|");
    const workersKey = (snapshot.workers || []).map((w) => `${w.id}@${w.node}`).sort().join("|");
    if (nodesKey === lastNodesKey && workersKey === lastWorkersKey && snapshot.self === lastSelf) {
      return;
    }
    lastNodesKey = nodesKey;
    lastWorkersKey = workersKey;
    lastSelf = snapshot.self || "";

    const workersByNode = {};
    (snapshot.workers || []).forEach((w) => {
      workersByNode[w.node] = workersByNode[w.node] || [];
      workersByNode[w.node].push(w.id);
    });

    nodesEl.innerHTML = (snapshot.nodes || [])
      .map((node) => {
        const workers = workersByNode[node] || [];
        const self = node === lastSelf;
        const workerHtml = workers.length
          ? workers
              .map(
                (id) =>
                  `<span class="worker" data-worker="${id}"><span class="chip"></span>${id}</span>`
              )
              .join("")
          : `<span class="empty">no workers yet</span>`;
        return `<article class="node${self ? " self" : ""}" data-node="${node}">
          <div class="node-title"><strong>${shortNode(node)}</strong><span>${self ? "this HTTP" : "member"}</span></div>
          <div class="workers">${workerHtml}</div>
        </article>`;
      })
      .join("");
  }

  function ensureFeedScaffold() {
    if (!feedEl.dataset.ready) {
      feedEl.innerHTML = "";
      feedEl.dataset.ready = "1";
    }
  }

  function renderFeed(snapshot) {
    ensureFeedScaffold();
    const events = snapshot.events || [];
    if (!events.length) {
      if (!feedEl.querySelector(".empty")) {
        feedEl.innerHTML = `<div class="empty">No traffic yet. Send a message.</div>`;
        feedEl.dataset.ready = "";
      }
      return;
    }

    const empty = feedEl.querySelector(".empty");
    if (empty) empty.remove();
    feedEl.dataset.ready = "1";

    const fresh = [];
    for (const ev of events) {
      const key = eventKey(ev);
      if (!knownEventKeys.has(key)) {
        knownEventKeys.add(key);
        fresh.push(ev);
      }
    }

    for (const ev of fresh.reverse()) {
      const peer = ev.kind === "RECEIVED" ? `from ${ev.otherWorkerId}` : `to ${ev.otherWorkerId}`;
      const article = document.createElement("article");
      article.className = "event is-new";
      article.dataset.key = eventKey(ev);
      article.innerHTML = `<div class="kind ${ev.kind}">${ev.kind}</div>
        <div class="event-body">
          <div class="line"><strong>${ev.workerId}</strong> on ${shortNode(ev.node)} · ${peer}</div>
          <div class="meta">${ev.body || ""} · ${ev.correlationId || ""}</div>
        </div>`;
      feedEl.prepend(article);
      const workerEl = document.querySelector(`.worker[data-worker="${ev.workerId}"]`);
      const nodeEl = document.querySelector(`.node[data-node="${CSS.escape(ev.node)}"]`);
      flash(workerEl, `flash-${ev.kind.toLowerCase()}`);
      flash(nodeEl, "flash");
      setTimeout(() => article.classList.remove("is-new"), 250);
    }

    while (feedEl.children.length > 40) {
      feedEl.removeChild(feedEl.lastElementChild);
    }

    if (knownEventKeys.size > 500) {
      knownEventKeys = new Set(Array.from(knownEventKeys).slice(-200));
    }
  }

  function render(snapshot) {
    statusEl.classList.add("live");
    if (!sending) {
      statusText.textContent = `live · ${shortNode(snapshot.self || "")}`;
    }
    nodeCountEl.textContent = `${(snapshot.nodes || []).length} up`;
    renderNodes(snapshot);
    renderFeed(snapshot);
  }

  async function pull() {
    try {
      const res = await fetch("/api/snapshot");
      if (!res.ok) throw new Error("snapshot failed");
      render(await res.json());
    } catch (err) {
      statusEl.classList.remove("live");
      if (!sending) statusText.textContent = "offline";
    }
  }

  sendBtn.addEventListener("click", async () => {
    const from = document.getElementById("from").value.trim() || "w1";
    const to = document.getElementById("to").value.trim() || "w2";
    const body = document.getElementById("body").value.trim() || "ping";
    sendBtn.disabled = true;
    sending = true;
    try {
      const res = await fetch(`/api/send?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body
      });
      const json = await res.json();
      statusText.textContent = `${json.status || res.status} · ${json.correlationId || ""}`;
      await pull();
    } catch (err) {
      statusText.textContent = "send failed";
    } finally {
      sending = false;
      sendBtn.disabled = false;
    }
  });

  pull();
  setInterval(pull, 1000);
})();

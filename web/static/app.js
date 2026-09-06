// Nutanix-Sync Minimalist Editorial Telemetry & Schematic Engine
let ws = null;
let clusterState = null;
let selectedNodeId = null;

const canvas = document.getElementById("topologyCanvas");
const ctx = canvas ? canvas.getContext("2d") : null;

let particles = [];
let animFrameId = null;

// DOM Elements
const wsStatusBadge = document.getElementById("wsStatusBadge");
const wsStatusText = document.getElementById("wsStatusText");
const clusterConsistencyBadge = document.getElementById("clusterConsistencyBadge");
const clusterConsistencyText = document.getElementById("clusterConsistencyText");

const metricAliveNodes = document.getElementById("metricAliveNodes");
const metricTotalNodes = document.getElementById("metricTotalNodes");
const metricBandwidthSavedPct = document.getElementById("metricBandwidthSavedPct");
const metricBytesSaved = document.getElementById("metricBytesSaved");
const metricBytesTransferred = document.getElementById("metricBytesTransferred");
const metricConflictsCount = document.getElementById("metricConflictsCount");

const filesTableBody = document.getElementById("filesTableBody");
const catalogFilesCount = document.getElementById("catalogFilesCount");
const terminalLog = document.getElementById("terminalLog");

const btnViewCanvas = document.getElementById("btnViewCanvas");
const btnViewGrid = document.getElementById("btnViewGrid");
const canvasContainer = document.getElementById("canvasContainer");
const nodeGridView = document.getElementById("nodeGridView");

// Resize canvas to match display size
function resizeCanvas() {
  if (!canvas || !canvasContainer) return;
  const rect = canvasContainer.getBoundingClientRect();
  canvas.width = rect.width;
  canvas.height = rect.height;
}
window.addEventListener("resize", resizeCanvas);

// View Switching
if (btnViewCanvas && btnViewGrid) {
  btnViewCanvas.addEventListener("click", () => {
    btnViewCanvas.classList.add("active");
    btnViewGrid.classList.remove("active");
    canvasContainer.classList.remove("hidden");
    nodeGridView.classList.add("hidden");
  });

  btnViewGrid.addEventListener("click", () => {
    btnViewGrid.classList.add("active");
    btnViewCanvas.classList.remove("active");
    canvasContainer.classList.add("hidden");
    nodeGridView.classList.remove("hidden");
    renderNodeGrid();
  });
}

// WebSocket Connection
function connectWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${window.location.host}/ws/cluster`;

  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    if (wsStatusBadge) {
      wsStatusBadge.className = "pill-metric connected";
      wsStatusText.textContent = "TELEMETRY LIVE";
    }
    appendLog("SYNC", "Decentralized gossip mesh connected to localhost daemon.");
  };

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      updateClusterUI(data);
    } catch (e) {
      console.error("WS Parse error", e);
    }
  };

  ws.onclose = () => {
    if (wsStatusBadge) {
      wsStatusBadge.className = "pill-metric";
      wsStatusText.textContent = "RECONNECTING";
    }
    setTimeout(connectWebSocket, 2000);
  };
}

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function appendLog(tag, message) {
  if (!terminalLog) return;
  const now = new Date().toLocaleTimeString();
  const line = document.createElement("div");
  line.className = "journal-entry";

  let tagClass = "j-tag-sync";
  if (tag === "DELTA") tagClass = "j-tag-delta";
  if (tag === "WARN") tagClass = "j-tag-warn";
  if (tag === "CHAOS") tagClass = "j-tag-chaos";

  line.innerHTML = `<span class="j-ts">[${now}]</span> <span class="j-tag ${tagClass}">[${tag}]</span> ${message}`;
  terminalLog.appendChild(line);
  terminalLog.scrollTop = terminalLog.scrollHeight;
}

function clearLogs() {
  if (terminalLog) terminalLog.innerHTML = "";
}

// Update UI
function updateClusterUI(state) {
  clusterState = state;

  // Metrics
  if (metricAliveNodes) metricAliveNodes.textContent = state.alive_nodes;
  if (metricTotalNodes) metricTotalNodes.textContent = state.total_nodes;
  if (metricBandwidthSavedPct) metricBandwidthSavedPct.textContent = `${state.bandwidth_saved_pct}%`;
  if (metricBytesSaved) metricBytesSaved.textContent = `${formatBytes(state.total_bytes_saved)} SAVED`;
  if (metricBytesTransferred) metricBytesTransferred.textContent = formatBytes(state.total_bytes_transferred);
  if (metricConflictsCount) metricConflictsCount.textContent = state.total_conflicts;

  // Consistency Badge
  if (clusterConsistencyBadge && clusterConsistencyText) {
    if (state.is_consistent) {
      clusterConsistencyBadge.className = "pill-metric consistent";
      clusterConsistencyText.textContent = `100% CONVERGED (${state.alive_nodes} NODES)`;
    } else if (state.distinct_root_hashes > 1) {
      clusterConsistencyBadge.className = "pill-metric inconsistent";
      clusterConsistencyText.textContent = `CONVERGING (${state.distinct_root_hashes} ROOTS)`;
    } else {
      clusterConsistencyBadge.className = "pill-metric";
      clusterConsistencyText.textContent = "CLUSTER IDLE";
    }
  }

  // Update Files Table
  const onlineNode = state.nodes.find(n => n.status === "ONLINE");
  if (filesTableBody) {
    if (onlineNode && onlineNode.files && onlineNode.files.length > 0) {
      if (catalogFilesCount) catalogFilesCount.textContent = `${onlineNode.files.length} FILES`;
      filesTableBody.innerHTML = onlineNode.files.map(f => `
        <tr>
          <td class="table-path">${f.rel_path}</td>
          <td>${formatBytes(f.size)}</td>
          <td>${f.chunks} blk</td>
          <td class="table-hash" title="${f.full_hash}">${f.full_hash}</td>
          <td><code>${JSON.stringify(f.vector_clock)}</code></td>
          <td>${f.last_modified_by || 'local'}</td>
        </tr>
      `).join("");
    } else {
      if (catalogFilesCount) catalogFilesCount.textContent = "0 FILES";
      filesTableBody.innerHTML = `<tr><td colspan="6" class="table-empty">Directory idle. Inject a file to begin synchronized tracking.</td></tr>`;
    }
  }

  if (nodeGridView && !nodeGridView.classList.contains("hidden")) {
    renderNodeGrid();
  }
}

function renderNodeGrid() {
  if (!clusterState || !nodeGridView) return;
  nodeGridView.innerHTML = clusterState.nodes.map(n => `
    <div class="node-card ${n.status.toLowerCase()}">
      <div class="node-card-head">
        <span>${n.node_id}</span>
        <span>${n.status}</span>
      </div>
      <div class="node-hash" title="${n.root_hash}">MERKLE: ${n.root_hash_short || 'EMPTY'}</div>
      <div class="node-stats">
        <span>FILES: ${n.files_count}</span>
        <span>PEERS: ${n.peers ? n.peers.length : 0}</span>
      </div>
      <div class="node-stats">
        <span>TX: ${formatBytes(n.bytes_transferred || 0)}</span>
        <span>SAVED: ${formatBytes(n.bytes_saved || 0)}</span>
      </div>
    </div>
  `).join("");
}

// Architectural Schematic Canvas (No neon glow, clean technical line drawing)
function drawTopology() {
  if (!canvas || !ctx || (canvasContainer && canvasContainer.classList.contains("hidden"))) {
    animFrameId = requestAnimationFrame(drawTopology);
    return;
  }

  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (!clusterState || !clusterState.nodes || clusterState.nodes.length === 0) {
    ctx.fillStyle = "#4e5562";
    ctx.font = "12px 'JetBrains Mono', monospace";
    ctx.textAlign = "center";
    ctx.fillText("ESTABLISHING SCHEMATIC TOPOLOGY...", canvas.width / 2, canvas.height / 2);
    animFrameId = requestAnimationFrame(drawTopology);
    return;
  }

  const nodes = clusterState.nodes;
  const count = nodes.length;
  const centerX = canvas.width / 2;
  const centerY = canvas.height / 2;
  const radius = Math.min(canvas.width, canvas.height) * 0.36;

  // Compute node coordinates
  const positions = {};
  nodes.forEach((n, idx) => {
    const angle = (idx / count) * 2 * Math.PI - Math.PI / 2;
    positions[n.node_id] = {
      x: Math.round(centerX + radius * Math.cos(angle)),
      y: Math.round(centerY + radius * Math.sin(angle)),
      node: n
    };
  });

  // 1. Draw Gossip Links (Crisp 1px lines)
  ctx.lineWidth = 1;
  ctx.strokeStyle = "rgba(139, 146, 158, 0.16)";

  if (clusterState.topology_links) {
    clusterState.topology_links.forEach(link => {
      const src = positions[link.source];
      const tgt = positions[link.target];
      if (src && tgt) {
        ctx.beginPath();
        ctx.moveTo(src.x, src.y);
        ctx.lineTo(tgt.x, tgt.y);
        ctx.stroke();
      }
    });
  }

  // 2. Draw Precision Pulse Particles
  if (Math.random() < 0.12 && clusterState.topology_links && clusterState.topology_links.length > 0) {
    const randLink = clusterState.topology_links[Math.floor(Math.random() * clusterState.topology_links.length)];
    const src = positions[randLink.source];
    const tgt = positions[randLink.target];
    if (src && tgt) {
      particles.push({
        x: src.x,
        y: src.y,
        targetX: tgt.x,
        targetY: tgt.y,
        progress: 0,
        speed: 0.025 + Math.random() * 0.02,
        color: clusterState.is_consistent ? "#f5f6f8" : "#ff3b00"
      });
    }
  }

  for (let i = particles.length - 1; i >= 0; i--) {
    const p = particles[i];
    p.progress += p.speed;
    const curX = p.x + (p.targetX - p.x) * p.progress;
    const curY = p.y + (p.targetY - p.y) * p.progress;

    // Small sharp square packet
    ctx.fillStyle = p.color;
    ctx.fillRect(curX - 1.5, curY - 1.5, 3, 3);

    if (p.progress >= 1) {
      particles.splice(i, 1);
    }
  }

  // 3. Draw Nodes (Architectural drafting markers)
  nodes.forEach(n => {
    const pos = positions[n.node_id];
    if (!pos) return;

    const isOnline = n.status === "ONLINE";

    // Crosshair lines through node center
    ctx.strokeStyle = "rgba(139, 146, 158, 0.35)";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(pos.x - 12, pos.y);
    ctx.lineTo(pos.x + 12, pos.y);
    ctx.moveTo(pos.x, pos.y - 12);
    ctx.lineTo(pos.x, pos.y + 12);
    ctx.stroke();

    if (isOnline) {
      // Outer 1px precise ring
      ctx.beginPath();
      ctx.arc(pos.x, pos.y, 8, 0, Math.PI * 2);
      ctx.fillStyle = "#0c0d10";
      ctx.fill();
      ctx.strokeStyle = "#f5f6f8";
      ctx.stroke();

      // Inner solid center
      ctx.beginPath();
      ctx.arc(pos.x, pos.y, 3, 0, Math.PI * 2);
      ctx.fillStyle = "#f5f6f8";
      ctx.fill();
    } else {
      // Severed node: sharp square with safety vermillion accent
      ctx.fillStyle = "#ff3b00";
      ctx.fillRect(pos.x - 4, pos.y - 4, 8, 8);
    }

    // Monospace Technical Node Label
    ctx.fillStyle = isOnline ? "#f5f6f8" : "#ff3b00";
    ctx.font = "10px 'JetBrains Mono', monospace";
    ctx.textAlign = "center";
    ctx.fillText(n.node_id.toUpperCase(), pos.x, pos.y + 22);
  });

  animFrameId = requestAnimationFrame(drawTopology);
}

// REST Control Actions
async function spawnScale(count) {
  appendLog("CHAOS", `Configuring cluster topology to ${count} virtual nodes...`);
  try {
    const res = await fetch("/api/cluster/spawn", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ node_count: count })
    });
    const data = await res.json();
    appendLog("SYNC", `Topology re-initialized with ${data.node_count} nodes. Gossip mesh converging.`);
  } catch (e) {
    appendLog("WARN", `Topology error: ${e.message}`);
  }
}

async function triggerBroadcastFile() {
  const relPathInput = document.getElementById("inputRelPath");
  const sizeSelect = document.getElementById("selectFileSize");
  const relPath = (relPathInput && relPathInput.value) || "dataset/enterprise_core.bin";
  const sizeKb = parseInt((sizeSelect && sizeSelect.value) || 256);

  appendLog("SYNC", `Injecting ${sizeKb} KB dataset at Node 00: ${relPath}`);
  try {
    const res = await fetch("/api/cluster/inject_file", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        node_index: 0,
        rel_path: relPath,
        size_kb: sizeKb
      })
    });
    const data = await res.json();
    appendLog("DELTA", `Block manifest registered: ${data.details.hash.substring(0, 16)}... Swarm propagation underway.`);
  } catch (e) {
    appendLog("WARN", `Broadcast error: ${e.message}`);
  }
}

async function triggerMutateDelta() {
  const relPathInput = document.getElementById("inputRelPath");
  const relPath = (relPathInput && relPathInput.value) || "dataset/enterprise_core.bin";
  appendLog("DELTA", `Mutating 64 bytes inside ${relPath} on Node 00...`);
  try {
    const res = await fetch("/api/cluster/mutate_delta", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        node_index: 0,
        rel_path: relPath,
        mutation_text: "[HOT_DELTA_UPDATE_SHA256_TEST_BLOCK_001]",
        offset: 500
      })
    });
    const data = await res.json();
    appendLog("DELTA", `Delta mutation committed. Merkle diff computed: only 1 chunk transmitted.`);
  } catch (e) {
    appendLog("WARN", `Delta mutation error: ${e.message}`);
  }
}

async function triggerConflict() {
  appendLog("WARN", `Triggering concurrent split-brain write between Node 00 & Node 01...`);
  try {
    const res = await fetch("/api/cluster/inject_conflict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        node_a_idx: 0,
        node_b_idx: 1,
        rel_path: "config/cluster_settings.json",
        content_a: '{"master": "node-00", "region": "US-EAST", "version": 1.1}',
        content_b: '{"master": "node-01", "region": "EU-WEST", "version": 2.0}'
      })
    });
    const data = await res.json();
    appendLog("WARN", `Vector Clock divergence detected. Non-destructive conflict branch preserved.`);
  } catch (e) {
    appendLog("WARN", `Conflict error: ${e.message}`);
  }
}

async function triggerKillNode() {
  if (!clusterState || !clusterState.nodes) return;
  const onlineNodes = clusterState.nodes.map((n, idx) => ({ ...n, idx })).filter(n => n.status === "ONLINE");
  if (onlineNodes.length <= 1) {
    appendLog("WARN", "Preserving minimum operational node.");
    return;
  }
  const target = onlineNodes[Math.floor(Math.random() * onlineNodes.length)];
  appendLog("CHAOS", `Severing link to ${target.node_id}...`);
  try {
    await fetch("/api/cluster/kill_node", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ node_index: target.idx })
    });
    appendLog("CHAOS", `${target.node_id} offline. SWIM failure detector tracking heartbeat loss.`);
  } catch (e) {
    appendLog("WARN", `Kill error: ${e.message}`);
  }
}

async function triggerReviveNode() {
  if (!clusterState || !clusterState.nodes) return;
  const offlineNodes = clusterState.nodes.map((n, idx) => ({ ...n, idx })).filter(n => n.status === "OFFLINE");
  if (offlineNodes.length === 0) {
    appendLog("SYNC", "All nodes online.");
    return;
  }
  const target = offlineNodes[0];
  appendLog("CHAOS", `Re-connecting ${target.node_id}...`);
  try {
    await fetch("/api/cluster/revive_node", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ node_index: target.idx })
    });
    appendLog("SYNC", `${target.node_id} re-joined. Merkle root synchronizing from nearest alive peer.`);
  } catch (e) {
    appendLog("WARN", `Revive error: ${e.message}`);
  }
}

// Initialize on page load
window.addEventListener("DOMContentLoaded", () => {
  resizeCanvas();
  connectWebSocket();
  drawTopology();
});

// Dashboard chainsim: semua data dirender dengan textContent (AGENTS.md Bagian 10).
"use strict";

const REFRESH_MS = Number(document.body.dataset.refreshMs) || 3000;
const EVENT_LIMIT = 30;

const connectionEl = document.getElementById("connection");
const nodesBody = document.getElementById("nodes-body");
const nodesEmpty = document.getElementById("nodes-empty");
const eventsList = document.getElementById("events-list");
const eventsEmpty = document.getElementById("events-empty");
const lastRefresh = document.getElementById("last-refresh");

function formatNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "-";
}

function formatInteger(value) {
  return Number.isInteger(value) ? String(value) : "-";
}

function formatHash(value) {
  if (typeof value !== "string" || value.length === 0) {
    return "-";
  }
  return value.length > 14 ? `${value.slice(0, 14)}...` : value;
}

function formatTime(tsMs) {
  if (typeof tsMs !== "number") {
    return "-";
  }
  return new Date(tsMs).toLocaleTimeString("id-ID", { hour12: false });
}

function makeCell(text, className) {
  const cell = document.createElement("td");
  cell.textContent = text;
  if (className) {
    cell.className = className;
  }
  return cell;
}

function statePill(state) {
  const pill = document.createElement("span");
  const normalized = state === "up" || state === "down" ? state : "unknown";
  pill.className = `pill ${normalized}`;
  pill.textContent = normalized;
  return pill;
}

function renderNodes(nodes) {
  nodesBody.replaceChildren();
  nodesEmpty.hidden = nodes.length > 0;
  for (const node of nodes) {
    const row = document.createElement("tr");
    row.append(makeCell(node.node_id, "mono"));
    const stateCell = document.createElement("td");
    stateCell.append(statePill(node.state));
    row.append(stateCell);
    row.append(makeCell(formatInteger(node.height)));
    row.append(makeCell(formatHash(node.tip_hash), "mono"));
    row.append(makeCell(formatInteger(node.mempool_size)));
    row.append(makeCell(formatNumber(node.hashrate)));
    const peers = Array.isArray(node.peers) ? node.peers.length : 0;
    row.append(makeCell(String(peers)));
    nodesBody.append(row);
  }
}

function renderEvents(events) {
  eventsList.replaceChildren();
  eventsEmpty.hidden = events.length > 0;
  for (const event of events) {
    const item = document.createElement("li");
    const time = document.createElement("time");
    time.textContent = formatTime(event.ts_ms);
    const name = document.createElement("strong");
    name.textContent = String(event.event ?? "unknown");
    const source = document.createElement("span");
    source.className = "mono";
    const target = event.target ? ` -> ${event.target}` : "";
    source.textContent = ` ${event.node_id ?? "?"}${target}`;
    item.append(time, document.createTextNode(" "), name, source);
    eventsList.append(item);
  }
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store", headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function setConnection(text, isError) {
  connectionEl.textContent = text;
  connectionEl.classList.toggle("error", Boolean(isError));
}

async function refresh() {
  try {
    const [nodesPayload, eventsPayload] = await Promise.all([
      fetchJson("/api/nodes"),
      fetchJson(`/api/events?limit=${EVENT_LIMIT}`),
    ]);
    renderNodes(Array.isArray(nodesPayload.nodes) ? nodesPayload.nodes : []);
    renderEvents(Array.isArray(eventsPayload.events) ? eventsPayload.events : []);
    setConnection("Terhubung ke coordinator", false);
    lastRefresh.textContent = `Diperbarui ${new Date().toLocaleTimeString("id-ID", { hour12: false })}`;
  } catch (error) {
    setConnection(`Gagal membaca data: ${error.message}`, true);
  }
}

refresh();
setInterval(refresh, REFRESH_MS);

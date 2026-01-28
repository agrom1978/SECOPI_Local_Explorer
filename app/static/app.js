const statusLine = document.getElementById("status-line");
const totalPill = document.getElementById("total-pill");
const resultsTable = document.getElementById("results-table");
const tableHead = resultsTable.querySelector("thead");
const tableBody = resultsTable.querySelector("tbody");
const pagerInfo = document.getElementById("pager-info");
const alertBox = document.getElementById("alert-box");
const statTotalTop = document.getElementById("stat-total-top");
const statPageTop = document.getElementById("stat-page-top");
const statSyncTop = document.getElementById("stat-sync-top");
const statSyncStatus = document.getElementById("stat-sync-status");

const btnPrev = document.getElementById("btn-prev");
const btnNext = document.getElementById("btn-next");

const fields = {
  anno: "anno",
  annoMin: "anno-min",
  annoMax: "anno-max",
  modalidad: "modalidad",
  destino: "destino",
  entidad: "entidad",
  cuantiaMin: "cuantia-min",
  cuantiaMax: "cuantia-max",
  estado: "estado",
  q: "q",
  limit: "limit",
  offset: "offset",
  cols: "cols",
  xlsxLimit: "xlsx-limit",
};

const visibleColumns = [
  "uid",
  "anno_firma_contrato",
  "modalidad_de_contratacion",
  "estado_del_proceso",
  "cuantia_proceso",
  "nombre_entidad",
  "dataset_updated_at",
];

function val(id) {
  const el = document.getElementById(id);
  return el ? el.value.trim() : "";
}

function buildQuery() {
  const params = new URLSearchParams();
  const mapping = {
    anno: val(fields.anno),
    anno_min: val(fields.annoMin),
    anno_max: val(fields.annoMax),
    modalidad: val(fields.modalidad),
    destino: val(fields.destino),
    entidad: val(fields.entidad),
    cuantia_min: val(fields.cuantiaMin),
    cuantia_max: val(fields.cuantiaMax),
    estado: val(fields.estado),
    q: val(fields.q),
    limit: val(fields.limit) || "25",
    offset: val(fields.offset) || "0",
  };

  Object.entries(mapping).forEach(([key, value]) => {
    if (value !== "") params.set(key, value);
  });

  return params;
}

function renderTable(items) {
  const cols = items.length ? Object.keys(items[0]) : visibleColumns;
  const selected = cols.filter((c) => visibleColumns.includes(c));

  tableHead.innerHTML = "";
  tableBody.innerHTML = "";

  const headerRow = document.createElement("tr");
  selected.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  tableHead.appendChild(headerRow);

  items.forEach((row) => {
    const tr = document.createElement("tr");
    selected.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = row[col] ?? "";
      tr.appendChild(td);
    });
    tableBody.appendChild(tr);
  });
}

function showAlert(msg) {
  if (!msg) {
    alertBox.classList.add("hidden");
    alertBox.textContent = "";
    return;
  }
  alertBox.classList.remove("hidden");
  alertBox.textContent = msg;
}

async function loadProcesos() {
  statusLine.textContent = "Estado: cargando...";
  const params = buildQuery();
  const res = await fetch(`/procesos?${params.toString()}`);
  if (!res.ok) {
    statusLine.textContent = "Estado: error";
    showAlert("No se pudo cargar /procesos. Revisa el servidor.");
    return;
  }
  const data = await res.json();
  totalPill.textContent = `Total: ${data.total}`;
  const limit = Number(params.get("limit") || 25);
  const offset = Number(params.get("offset") || 0);
  const page = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil((data.total || 0) / limit));
  pagerInfo.textContent = `Pagina ${page} de ${pages}`;
  statTotalTop.textContent = data.total ?? "-";
  statPageTop.textContent = `${page} / ${pages}`;
  btnPrev.disabled = offset <= 0;
  btnNext.disabled = page >= pages;
  renderTable(data.items || []);
  statusLine.textContent = "Estado: listo";
  showAlert("");
}

async function loadStats() {
  const params = buildQuery();
  const res = await fetch(`/stats/resumen?${params.toString()}`);
  if (!res.ok) {
    showAlert("No se pudo cargar el resumen.");
    return;
  }
  const data = await res.json();
  document.getElementById("stat-total").textContent = data.total ?? "-";
  document.getElementById("stat-cuantia-proceso").textContent = data.total_cuantia_proceso ?? "-";
  document.getElementById("stat-cuantia-contrato").textContent = data.total_cuantia_contrato ?? "-";
  document.getElementById("stat-anno-min").textContent = data.min_anno_firma_contrato ?? "-";
  document.getElementById("stat-anno-max").textContent = data.max_anno_firma_contrato ?? "-";
}

async function loadStatus() {
  const res = await fetch("/sync/status");
  if (!res.ok) {
    showAlert("No se pudo cargar el estado de sync.");
    return;
  }
  const data = await res.json();
  statSyncTop.textContent = data.last_run_ts ? new Date(data.last_run_ts).toLocaleString() : "-";
  statSyncStatus.textContent = data.last_run_status ?? "-";
}

function clearFilters() {
  Object.values(fields).forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (id === fields.limit) return;
    if (id === fields.offset) return;
    if (id === fields.xlsxLimit) return;
    el.value = "";
  });
  document.getElementById(fields.limit).value = "25";
  document.getElementById(fields.offset).value = "0";
}

function exportCsv() {
  const params = buildQuery();
  const cols = val(fields.cols);
  if (cols) params.set("cols", cols);
  window.location.href = `/export/csv?${params.toString()}`;
}

function exportXlsx() {
  const params = buildQuery();
  const cols = val(fields.cols);
  const limit = val(fields.xlsxLimit) || "5000";
  if (cols) params.set("cols", cols);
  params.set("limit", limit);
  window.location.href = `/export/xlsx?${params.toString()}`;
}

function setOffset(newOffset) {
  const offsetInput = document.getElementById(fields.offset);
  offsetInput.value = String(Math.max(0, newOffset));
}

btnPrev.addEventListener("click", () => {
  const limit = Number(val(fields.limit) || 25);
  const offset = Number(val(fields.offset) || 0);
  setOffset(offset - limit);
  loadProcesos();
});

btnNext.addEventListener("click", () => {
  const limit = Number(val(fields.limit) || 25);
  const offset = Number(val(fields.offset) || 0);
  setOffset(offset + limit);
  loadProcesos();
});

document.getElementById("btn-search").addEventListener("click", loadProcesos);
document.getElementById("btn-refresh").addEventListener("click", loadProcesos);
document.getElementById("btn-stats").addEventListener("click", loadStats);
document.getElementById("btn-status").addEventListener("click", loadStatus);
document.getElementById("btn-clear").addEventListener("click", () => {
  clearFilters();
  loadProcesos();
});
document.getElementById("btn-export-csv").addEventListener("click", exportCsv);
document.getElementById("btn-export-xlsx").addEventListener("click", exportXlsx);

document.querySelectorAll(".chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    document.querySelectorAll(".chip").forEach((c) => c.classList.remove("active"));
    chip.classList.add("active");
    const estadoValue = chip.getAttribute("data-estado");
    document.getElementById(fields.estado).value = estadoValue || "";
    loadProcesos();
  });
});

loadProcesos();
loadStatus();

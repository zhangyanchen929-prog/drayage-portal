const state = {
  overviewStatus: "all",
  shipmentStatus: "all",
  shipmentSelected: new Set(),
  doSelected: new Set(),
};
let authToken = localStorage.getItem("drayage_token") || "";
let currentUser = null;

const STATUS_KEYS = [
  ["total", "Total"],
  ["awaiting_dispatch", "Awaiting Dispatch"],
  ["pending", "Pending"],
  ["scheduled", "Scheduled"],
  ["exam_on_hold", "Exam/On Hold"],
  ["pre_pull", "Pre-pull"],
  ["dispatched", "Dispatched"],
  ["delivered", "Delivered"],
  ["empty_date_confirmed", "Empty Date Confirmed"],
  ["closed", "Closed"],
];

const CARD_META = {
  total: { sub: "All shipments", chip: "Total" },
  awaiting_dispatch: { sub: "Next-Day pickups", chip: "Next-Day" },
  pending: { sub: "Status · Pending", chip: "Pending" },
  scheduled: { sub: "Status · Scheduled", chip: "Scheduled" },
  exam_on_hold: { sub: "Status · Exam/On Hold", chip: "Exam/On Hold" },
  pre_pull: { sub: "Pre-pull shipments", chip: "Pre-pull" },
  dispatched: { sub: "Status · Dispatched", chip: "Dispatched" },
  delivered: { sub: "Status · Delivered", chip: "Delivered" },
  empty_date_confirmed: { sub: "Status · Empty Date Confirmed", chip: "Empty Date Confirmed" },
  closed: { sub: "Status · Closed", chip: "Closed" },
};

function q(id) {
  return document.getElementById(id);
}

function closeShipmentModal() {
  q("shipment-modal").classList.add("hidden");
}

function openShipmentModal() {
  q("shipment-modal").classList.remove("hidden");
}

function resetShipmentForm() {
  q("shipment-form").reset();
  q("f-size").value = "40HC";
  q("f-status").value = "awaiting_dispatch";
}

function closeDetailModal() {
  q("detail-modal").classList.add("hidden");
}

function openDetailModal() {
  q("detail-modal").classList.remove("hidden");
}

function closeTimeModal() {
  q("time-modal").classList.add("hidden");
}

function openTimeModal() {
  q("time-modal").classList.remove("hidden");
}

function toDateTimeLocal(value) {
  if (!value) return "";
  const normalized = String(value).replace(" ", "T");
  return normalized.length >= 16 ? normalized.slice(0, 16) : normalized;
}

function fromDateTimeLocal(value) {
  if (!value) return "";
  return value.replace("T", " ");
}

function showAppError(message) {
  const el = q("app-error");
  el.textContent = message;
  el.classList.remove("hidden");
}

async function api(path, options = {}) {
  const opts = { ...options };
  const noAuth = opts.noAuth === true;
  delete opts.noAuth;
  const headers = new Headers(opts.headers || {});
  if (!noAuth && authToken) {
    headers.set("Authorization", `Bearer ${authToken}`);
  }
  opts.headers = headers;
  const res = await fetch(path, opts);
  if (!res.ok) {
    if (res.status === 401) {
      showLogin();
    }
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  const type = res.headers.get("content-type") || "";
  if (type.includes("application/json")) return res.json();
  return res;
}

function showLogin() {
  document.body.classList.add("auth-mode");
  q("login-modal").classList.remove("hidden");
  q("login-password").value = "";
}

function hideLogin() {
  document.body.classList.remove("auth-mode");
  q("login-modal").classList.add("hidden");
  q("login-error").classList.add("hidden");
  q("login-error").textContent = "";
}

function applyRolePermissions() {
  const rolePill = q("role-pill");
  if (!currentUser) {
    rolePill.classList.add("hidden");
    return;
  }
  rolePill.textContent = currentUser.role;
  rolePill.classList.remove("hidden");
}

function fmtTime(v) {
  return v || "-";
}

function fmtDateOnly(v) {
  if (!v) return "-";
  const raw = String(v).trim();
  if (!raw) return "-";
  const normalized = raw.replace("T", " ");
  if (/^\d{4}-\d{2}-\d{2}/.test(normalized)) {
    const [y, m, d] = normalized.slice(0, 10).split("-");
    return `${m}/${d}/${y.slice(2)}`;
  }
  return normalized.split(" ")[0];
}

function statusBadge(status, label) {
  return `<span class="status ${status}">${label || status}</span>`;
}

function cardChip(status) {
  const label = CARD_META[status]?.chip || status;
  const className = status === "total" ? "closed" : status;
  return `<span class="chip status ${className}">${label}</span>`;
}

function bindNavigation() {
  document.querySelectorAll(".nav-link").forEach((btn) => {
    btn.addEventListener("click", () => {
      openPage(btn.dataset.page);
      if (btn.dataset.page === "shipment") {
        state.shipmentStatus = "all";
        refreshShipmentStatusTip();
        loadShipments();
      }
    });
  });
}

function openPage(page) {
  document.querySelectorAll(".nav-link").forEach((n) => n.classList.remove("active"));
  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  const nav = document.querySelector(`.nav-link[data-page='${page}']`);
  if (nav) nav.classList.add("active");
  q(`page-${page}`).classList.add("active");
}

function refreshShipmentStatusTip() {
  const tip = q("shipment-status-tip");
  const clearBtn = q("clear-shipment-status");
  if (state.shipmentStatus === "all") {
    tip.classList.add("hidden");
    clearBtn.classList.add("hidden");
    return;
  }
  const label = CARD_META[state.shipmentStatus]?.chip || state.shipmentStatus;
  tip.textContent = `Status: ${label}`;
  tip.classList.remove("hidden");
  clearBtn.classList.remove("hidden");
}

async function loadHeader() {
  if (!currentUser) return;
  q("email").textContent = currentUser.email;
  q("tz").textContent = currentUser.timezone;
  applyRolePermissions();
}

async function loadOverview() {
  const stats = await api("/api/overview/stats");
  q("overview-cards").innerHTML = STATUS_KEYS.map(([k, label]) => {
    const v = stats[k] ?? 0;
    const selected = (k === "total" && state.overviewStatus === "all") || state.overviewStatus === k;
    return `<div class="card ${selected ? "active" : ""}" data-status="${k === "total" ? "all" : k}">
      <div class="card-top">
        <div>
          <div class="card-title">${label}</div>
          <div class="card-sub">${CARD_META[k]?.sub || ""}</div>
        </div>
        ${cardChip(k)}
      </div>
      <strong>${v}</strong>
    </div>`;
  }).join("");

  document.querySelectorAll("#overview-cards .card").forEach((card) => {
    card.addEventListener("click", () => {
      const status = card.dataset.status;
      state.overviewStatus = status;
      state.shipmentStatus = status;
      openPage("shipment");
      refreshShipmentStatusTip();
      loadShipments();
    });
  });

  await loadOverviewTable();
}

async function loadOverviewTable() {
  const search = q("overview-search").value;
  const data = await api(`/api/overview/shipments?search=${encodeURIComponent(search)}&status=${encodeURIComponent(state.overviewStatus)}`);
  q("overview-table").innerHTML = data.items.map((r) => `
    <tr>
      <td><button data-shipment-id="${r.shipment_id}" class="open-detail">${r.shipment_id}</button></td>
      <td>${r.container_no}</td>
      <td>${fmtTime(r.eta_at)}</td>
      <td>${fmtTime(r.lfd_at)}</td>
      <td>${statusBadge(r.status, r.status_label)}</td>
    </tr>
  `).join("");

  document.querySelectorAll(".open-detail").forEach((btn) => {
    btn.addEventListener("click", () => openShipmentDetail(btn.dataset.shipmentId));
  });
}

async function loadShipments() {
  const params = new URLSearchParams({
    search: q("shipment-search").value,
    status: state.shipmentStatus,
    sort: q("shipment-sort").value,
    today_pickup: q("today-pickups").checked,
    next_day_pickup: q("nextday-pickups").checked,
    pre_pull_only: q("prep-pull").checked,
  });
  const data = await api(`/api/shipments?${params.toString()}`);
  refreshShipmentStatusTip();

  q("shipment-table").innerHTML = data.items.map((s) => {
    const pod = s.pod ? `${s.pod.verify_status || "uploaded"} / ${s.pod.file_name}` : "-";
    const podState = s.pod ? (s.pod.verify_status || "Uploaded") : "Not uploaded";
    const checked = state.shipmentSelected.has(s.shipment_id);
    const isOperator = currentUser?.role === "operator";
    return `
      <tr class="${checked ? "row-selected" : ""}">
        <td><input class="shipment-select" data-shipment-id="${s.shipment_id}" type="checkbox" ${checked ? "checked" : ""} /></td>
        <td><button class="open-detail shipment-id-link" data-shipment-id="${s.shipment_id}">${s.shipment_id}</button></td>
        <td>${s.container_no}</td>
        <td>${s.size || "-"}</td>
        <td>${s.terminal || "-"}</td>
        <td>${fmtTime(s.eta_at)}</td>
        <td>${fmtTime(s.lfd_at)}</td>
        <td>${s.deliver_to || "-"}</td>
        <td>${fmtTime(s.pickup_appt_at)}</td>
        <td>${fmtDateOnly(s.scheduled_delivery_at)}</td>
        <td>${fmtDateOnly(s.actual_delivery_at)}</td>
        <td>${fmtTime(s.empty_return_at)}</td>
        <td>${statusBadge(s.status, s.status_label)}</td>
        <td>${podState}</td>
        <td>
          <div class="actions-wrap">
            <button class="open-detail" data-shipment-id="${s.shipment_id}">View</button>
            <button class="more-actions" data-shipment-id="${s.shipment_id}">...</button>
            <div class="actions-menu hidden" id="menu-${s.shipment_id}">
              <button class="action-item" data-action="update-times" data-shipment-id="${s.shipment_id}" ${isOperator ? "" : "disabled"}>Update Times</button>
              <button class="action-item" data-action="reupload-pod" data-shipment-id="${s.shipment_id}" ${isOperator ? "" : "disabled"}>Re-upload POD</button>
              <button class="action-item" data-action="preview-pod" data-shipment-id="${s.shipment_id}" data-pod-doc-id="${s.pod ? s.pod.id : ""}">Preview POD</button>
              <button class="action-item" data-action="download-do" data-shipment-id="${s.shipment_id}" data-do-doc-id="${s.do ? s.do.id : ""}">Download DO</button>
              <button class="action-item" data-action="report-issue" data-shipment-id="${s.shipment_id}">Report Issue</button>
              <button class="action-item" data-action="mark-exam" data-shipment-id="${s.shipment_id}" ${isOperator ? "" : "disabled"}>Mark as Exam</button>
            </div>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  const wrap = document.querySelector(".shipment-table-wrap");
  if (wrap) wrap.scrollLeft = 0;

  const selectAll = q("select-visible-shipments");
  const allVisibleIds = data.items.map((s) => s.shipment_id);
  const allSelected = allVisibleIds.length > 0 && allVisibleIds.every((id) => state.shipmentSelected.has(id));
  selectAll.checked = allSelected;

  document.querySelectorAll(".shipment-select").forEach((cb) => {
    cb.addEventListener("change", () => {
      const id = cb.dataset.shipmentId;
      if (cb.checked) state.shipmentSelected.add(id);
      else state.shipmentSelected.delete(id);
      loadShipments();
    });
  });

  document.querySelectorAll(".open-detail").forEach((btn) => btn.addEventListener("click", () => openShipmentDetail(btn.dataset.shipmentId)));
  document.querySelectorAll(".more-actions").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      document.querySelectorAll(".actions-menu").forEach((menu) => {
        if (menu.id !== `menu-${btn.dataset.shipmentId}`) {
          menu.classList.add("hidden");
          menu.style.top = "-9999px";
          menu.style.left = "-9999px";
        }
      });
      const menu = q(`menu-${btn.dataset.shipmentId}`);
      menu.classList.toggle("hidden");
      if (!menu.classList.contains("hidden")) {
        const btnRect = btn.getBoundingClientRect();
        const menuWidth = 220;
        const menuHeight = 250;
        let left = btnRect.right - menuWidth;
        if (left < 8) left = 8;
        if (left + menuWidth > window.innerWidth - 8) left = window.innerWidth - 8 - menuWidth;

        let top = btnRect.bottom + 6;
        if (top + menuHeight > window.innerHeight - 8) {
          top = btnRect.top - menuHeight - 6;
        }
        if (top < 8) top = 8;

        menu.style.left = `${left}px`;
        menu.style.top = `${top}px`;
      }
    });
  });
  document.querySelectorAll(".action-item").forEach((btn) => {
    btn.addEventListener("click", async () => {
      if (btn.disabled) return;
      const shipmentId = btn.dataset.shipmentId;
      const action = btn.dataset.action;
      q(`menu-${shipmentId}`).classList.add("hidden");

      if (action === "update-times") {
        const detail = await api(`/api/shipments/${shipmentId}`);
        q("time-shipment-id").value = detail.shipment_id;
        q("time-pu-appt").value = toDateTimeLocal(detail.pickup_appt_at);
        openTimeModal();
        return;
      }
      if (action === "reupload-pod") {
        await openShipmentDetail(shipmentId);
        return;
      }
      if (action === "preview-pod") {
        const podDocId = btn.dataset.podDocId;
        if (podDocId) {
          window.open(`/api/documents/${podDocId}/download`, "_blank");
        } else {
          alert("No POD file.");
        }
        return;
      }
      if (action === "download-do") {
        const doDocId = btn.dataset.doDocId;
        if (doDocId) {
          window.open(`/api/documents/${doDocId}/download`, "_blank");
        } else {
          alert("No DO file.");
        }
        return;
      }
      if (action === "report-issue") {
        await api("/api/tickets", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ shipment_id: shipmentId, category: "issue", description: "Reported from Shipment action menu" }),
        });
        alert("Ticket created.");
        await loadTickets();
        return;
      }
      if (action === "mark-exam") {
        await api(`/api/shipments/${shipmentId}/status`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ to_status: "exam_on_hold", note: "Marked from action menu" }),
        });
        await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn(), loadPODUpload()]);
      }
    });
  });
}

async function openShipmentDetail(shipmentId) {
  const d = await api(`/api/shipments/${shipmentId}`);
  q("detail-subtitle").textContent = d.shipment_id;
  q("detail-status").innerHTML = statusBadge(d.status, d.status_label);

  const doDoc = d.documents.DO;
  const podDoc = d.documents.POD;
  const isOperator = currentUser?.role === "operator";
  const isCustomer = currentUser?.role === "customer";
  const notApplied = "<span class='na'>Not applied</span>";
  const pickupDone = !!d.timeline.pickup;
  const deliveryDone = !!d.timeline.delivery_actual || ["delivered", "empty_date_confirmed", "empty_returned", "closed"].includes(d.status);
  const emptyDone = !!d.timeline.empty_return;

  const doActions = doDoc
    ? `
      <a href="/api/documents/${doDoc.id}/download" target="_blank">Download DO</a>
      ${isCustomer ? `<a href="#" id="reupload-do-link">Re-upload DO</a>` : ""}
    `
    : (isCustomer ? `<a href="#" id="upload-do-link">Upload DO</a>` : "");

  const podActions = podDoc
    ? `
      ${isOperator ? `<a href="#" id="reupload-pod-link">Re-upload POD</a>` : ""}
      <a href="/api/documents/${podDoc.id}/download" target="_blank">Preview POD</a>
    `
    : (isOperator ? `<a href="#" id="reupload-pod-link">Upload POD</a>` : "");

  q("detail-content").innerHTML = `
    <div class="detail-grid">
      <div class="detail-section">
        <h3 class="section-title">Booking Info</h3>
        <div class="kv">
          <div class="kv-label">Shipment ID</div><div class="kv-value">${d.shipment_id}</div>
          <div class="kv-label">Container#</div><div class="kv-value">${d.container_no}</div>
          <div class="kv-label">Size</div><div class="kv-value">${d.size || notApplied}</div>
          <div class="kv-label">B/L</div><div class="kv-value">${d.mbol || notApplied}</div>
          <div class="kv-label">Terminal</div><div class="kv-value">${d.terminal || notApplied}</div>
          <div class="kv-label">Carrier</div><div class="kv-value">${d.carrier || notApplied}</div>
          <div class="kv-label">ETA</div><div class="kv-value">${fmtTime(d.eta_at)}</div>
          <div class="kv-label">LFD</div><div class="kv-value">${d.lfd_at || notApplied}</div>
          <div class="kv-label">DG</div><div class="kv-value">${d.dg ? "Yes" : notApplied}</div>
        </div>
      </div>
      <div>
        <div class="detail-section">
          <h3 class="section-title">Delivery Info</h3>
          <div class="kv">
            <div class="kv-label">Company</div><div class="kv-value">${d.deliver_company || notApplied}</div>
            <div class="kv-label">Deliver To</div><div class="kv-value">${d.deliver_to || notApplied}</div>
            <div class="kv-label">Warehouse Contact</div><div class="kv-value">${d.warehouse_contact || notApplied}</div>
            <div class="kv-label">Warehouse Contact Phone</div><div class="kv-value">${d.warehouse_phone || notApplied}</div>
            <div class="kv-label">Remark</div><div class="kv-value">${d.remark || notApplied}</div>
          </div>
        </div>
        <div class="detail-section">
          <h3 class="section-title">Documents</h3>
          <div class="doc-row">
            <strong class="doc-kind">DO</strong>
            ${doDoc ? `<span class="doc-tag">Uploaded</span>` : "<span>-</span>"}
            <span class="doc-name">${doDoc ? doDoc.file_name : "No file"}</span>
            <span class="doc-actions">${doActions}</span>
          </div>
          <div class="doc-row">
            <strong class="doc-kind">POD</strong>
            ${podDoc ? `<span class="doc-tag">Uploaded</span>` : "<span>-</span>"}
            <span class="doc-name">${podDoc ? podDoc.file_name : "No file"}</span>
            <span class="doc-actions">${podActions}</span>
          </div>
          <input type="file" id="upload-do" class="hidden" />
          <input type="file" id="upload-pod" class="hidden" />
        </div>
      </div>
    </div>
    <div class="detail-section">
      <h3 class="section-title">Timeline</h3>
      <div class="timeline-flow">
        <div class="timeline-node ${pickupDone ? "done" : "pending"}">
          <div class="node-dot">${pickupDone ? "✓" : "−"}</div>
          <div class="node-title">Pickup</div>
          <div class="node-sub">${d.timeline.pickup || "Pending"}</div>
        </div>
        <div class="timeline-link"></div>
        <div class="timeline-node ${deliveryDone ? "done" : "pending"}">
          <div class="node-dot">${deliveryDone ? "✓" : "−"}</div>
          <div class="node-title">Delivery</div>
          <div class="node-sub">Scheduled: ${d.timeline.delivery_scheduled || "-"}</div>
          <div class="node-sub">Actual: ${d.timeline.delivery_actual || "-"}</div>
        </div>
        <div class="timeline-link"></div>
        <div class="timeline-node ${emptyDone ? "done" : "pending"}">
          <div class="node-dot">${emptyDone ? "✓" : "−"}</div>
          <div class="node-title">Empty Return</div>
          <div class="node-sub">${d.timeline.empty_return || "Pending"}</div>
        </div>
      </div>
      <div class="toolbar ${isOperator ? "" : "hidden"}" style="margin-top:12px;">
        <select id="next-status">
          <option value="awaiting_dispatch">Awaiting Dispatch</option>
          <option value="pending">Pending</option>
          <option value="scheduled">Scheduled</option>
          <option value="exam_on_hold">Exam/On Hold</option>
          <option value="pre_pull">Pre-pull</option>
          <option value="dispatched">Dispatched</option>
          <option value="delivered">Delivered</option>
          <option value="empty_date_confirmed">Empty Date Confirmed</option>
          <option value="empty_returned">Empty Returned</option>
          <option value="closed">Closed</option>
        </select>
        <button id="save-status">Save</button>
      </div>
    </div>
  `;

  openDetailModal();
  const doUpload = q("upload-do");
  if (doUpload) {
    doUpload.addEventListener("change", (e) => uploadDoc(shipmentId, "DO", e.target.files[0]));
  }
  const uploadDo = q("upload-do-link");
  if (uploadDo) {
    uploadDo.addEventListener("click", (e) => {
      e.preventDefault();
      q("upload-do").click();
    });
  }
  const reuploadDo = q("reupload-do-link");
  if (reuploadDo) {
    reuploadDo.addEventListener("click", (e) => {
      e.preventDefault();
      q("upload-do").click();
    });
  }
  const reupload = q("reupload-pod-link");
  if (reupload) {
    reupload.addEventListener("click", (e) => {
      e.preventDefault();
      q("upload-pod").click();
    });
  }
  q("upload-pod").addEventListener("change", (e) => uploadDoc(shipmentId, "POD", e.target.files[0]));
  q("save-status").addEventListener("click", async () => {
    await api(`/api/shipments/${shipmentId}/status`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ to_status: q("next-status").value }),
    });
    await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn(), loadPODUpload()]);
    alert("Status updated");
  });
}

async function uploadDoc(shipmentId, type, file) {
  if (!file) return;
  const form = new FormData();
  form.append("file", file);
  await api(`/api/shipments/${shipmentId}/documents/${type}`, { method: "POST", body: form });
  await Promise.all([loadShipments(), loadPODUpload()]);
  await openShipmentDetail(shipmentId);
}

async function submitTimeUpdate(e) {
  e.preventDefault();
  const shipmentId = q("time-shipment-id").value;
  await api(`/api/shipments/${shipmentId}/times`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      pickup_appt_at: fromDateTimeLocal(q("time-pu-appt").value),
    }),
  });
  closeTimeModal();
  await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn()]);
}

async function loadEmptyReturn() {
  const search = q("empty-search").value;
  const data = await api(`/api/empty-returns?search=${encodeURIComponent(search)}`);
  q("empty-table").innerHTML = data.items.map((r) => `
    <tr>
      <td><button class="open-detail shipment-id-link" data-shipment-id="${r.shipment_id}">${r.shipment_id}</button></td><td>${r.mbol || "-"}</td><td>${r.container_no}</td><td>${r.terminal || "-"}</td>
      <td>${r.deliver_to || "-"}</td><td>${fmtTime(r.delivered)}</td><td>${fmtTime(r.empty_date)}</td><td>${fmtTime(r.empty_returned)}</td>
      <td>${statusBadge(r.status, r.status_label)}</td>
      <td><button class="open-detail" data-shipment-id="${r.shipment_id}">View</button></td>
    </tr>
  `).join("");
  document.querySelectorAll("#page-empty-return .open-detail").forEach((btn) => btn.addEventListener("click", () => openShipmentDetail(btn.dataset.shipmentId)));
}

async function loadDOList() {
  const search = q("do-search").value;
  const data = await api(`/api/do-download/list?search=${encodeURIComponent(search)}`);
  q("do-table").innerHTML = data.items.map((r) => `
    <tr>
      <td><input type="checkbox" class="do-select" data-shipment-id="${r.shipment_id}" ${state.doSelected.has(r.shipment_id) ? "checked" : ""} /></td>
      <td>${r.shipment_id}</td>
      <td>${r.container_no}</td>
      <td>${r.file_name} ${r.downloaded ? "(Downloaded)" : ""}</td>
      <td><a href="/api/documents/${r.doc_id}/download" target="_blank">Download</a></td>
    </tr>
  `).join("");

  document.querySelectorAll(".do-select").forEach((c) => {
    c.addEventListener("change", () => {
      if (c.checked) state.doSelected.add(c.dataset.shipmentId);
      else state.doSelected.delete(c.dataset.shipmentId);
      q("do-selected-count").textContent = state.doSelected.size;
    });
  });
  q("do-selected-count").textContent = state.doSelected.size;
}

async function loadPODUpload() {
  const search = q("pod-search").value;
  const data = await api(`/api/pod-upload/list?search=${encodeURIComponent(search)}`);
  q("pod-empty").classList.toggle("hidden", data.items.length > 0);
  q("pod-table").innerHTML = data.items.map((r) => `
    <tr>
      <td>${r.shipment_id}</td><td>${r.container_no}</td><td>${r.mbol || "-"}</td><td>${r.terminal || "-"}</td><td>${fmtTime(r.eta_at)}</td><td>${fmtTime(r.lfd_at)}</td>
      <td>${r.pod_file_name ? `${r.verify_status || "uploaded"} / ${r.pod_file_name}` : "Not uploaded"}</td>
      <td><button class="open-detail" data-shipment-id="${r.shipment_id}">Upload / View</button></td>
    </tr>
  `).join("");
  document.querySelectorAll("#page-upload-pod .open-detail").forEach((btn) => btn.addEventListener("click", () => openShipmentDetail(btn.dataset.shipmentId)));
}

async function loadTickets() {
  const data = await api("/api/tickets");
  q("ticket-table").innerHTML = data.items.map((t) => `
    <tr><td>${t.ticket_no}</td><td>${t.shipment_id || "-"}</td><td>${t.category}</td><td>${t.attachment_name || "-"}</td><td>${t.status}</td><td>${t.created_at}</td></tr>
  `).join("");
}

async function loadPricing() {
  const data = await api("/api/pricing/rules");
  q("pricing-count").textContent = data.count;
  q("pricing-table").innerHTML = data.items.map((r) => `
    <tr>
      <td>${r.priority}</td><td>${r.code}</td><td>${r.label}</td><td>${r.calculator}</td><td>${r.amount}</td><td>${r.zone || "-"}</td><td>${r.container || "-"}</td><td>${r.free_days ?? "-"}</td><td>${r.free_hours ?? "-"}</td><td>${r.bill_to || "-"}</td>
    </tr>
  `).join("");
}

async function createShipmentFromForm(e) {
  e.preventDefault();
  const payload = {
    shipment_id: q("f-shipment-id").value,
    container_no: q("f-container-no").value,
    mbol: q("f-mbol").value,
    size: q("f-size").value,
    terminal: q("f-terminal").value,
    carrier: q("f-carrier").value,
    eta_at: q("f-eta").value,
    lfd_at: q("f-lfd").value,
    deliver_company: q("f-deliver-company").value,
    deliver_to: q("f-deliver-to").value,
    warehouse_contact: q("f-contact").value,
    warehouse_phone: q("f-phone").value,
    pickup_appt_at: q("f-pickup-appt").value,
    scheduled_delivery_at: q("f-sch-del").value,
    status: q("f-status").value,
    dg: q("f-dg").checked,
    remark: q("f-remark").value,
  };

  await api("/api/shipments", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });

  closeShipmentModal();
  resetShipmentForm();
  await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn(), loadPODUpload()]);
}

function bindEvents() {
  q("overview-search").addEventListener("input", loadOverviewTable);
  q("shipment-search").addEventListener("input", loadShipments);
  q("shipment-sort").addEventListener("change", loadShipments);
  q("time-form").addEventListener("submit", submitTimeUpdate);
  q("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const payload = {
        email: q("login-email").value.trim().toLowerCase(),
        password: q("login-password").value,
      };
      const result = await api("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
        noAuth: true,
      });
      authToken = result.token;
      currentUser = result.user;
      localStorage.setItem("drayage_token", authToken);
      hideLogin();
      await loadHeader();
      await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn(), loadDOList(), loadPODUpload(), loadTickets(), loadPricing()]);
    } catch (err) {
      q("login-error").textContent = "Invalid email or password";
      q("login-error").classList.remove("hidden");
    }
  });
  q("hero-signin-btn").addEventListener("click", () => {
    q("login-email").focus();
  });
  q("hero-quote-btn").addEventListener("click", () => {
    q("login-email").focus();
  });
  q("logout-btn").addEventListener("click", async () => {
    try {
      await api("/api/auth/logout", { method: "POST" });
    } catch (_) {}
    authToken = "";
    currentUser = null;
    localStorage.removeItem("drayage_token");
    showLogin();
  });
  q("select-visible-shipments").addEventListener("change", async (e) => {
    const checked = e.target.checked;
    const params = new URLSearchParams({
      search: q("shipment-search").value,
      status: state.shipmentStatus,
      sort: q("shipment-sort").value,
      today_pickup: q("today-pickups").checked,
      next_day_pickup: q("nextday-pickups").checked,
      pre_pull_only: q("prep-pull").checked,
    });
    const data = await api(`/api/shipments?${params.toString()}`);
    data.items.forEach((s) => {
      if (checked) state.shipmentSelected.add(s.shipment_id);
      else state.shipmentSelected.delete(s.shipment_id);
    });
    await loadShipments();
  });
  q("shipment-refresh").addEventListener("click", loadShipments);
  q("today-pickups").addEventListener("change", loadShipments);
  q("nextday-pickups").addEventListener("change", loadShipments);
  q("prep-pull").addEventListener("change", loadShipments);
  q("empty-search").addEventListener("input", loadEmptyReturn);
  q("do-search").addEventListener("input", loadDOList);
  q("pod-search").addEventListener("input", loadPODUpload);
  q("new-shipment-btn").addEventListener("click", openShipmentModal);
  q("shipment-form").addEventListener("submit", createShipmentFromForm);
  q("clear-shipment-status").addEventListener("click", async () => {
    state.shipmentStatus = "all";
    refreshShipmentStatusTip();
    await loadShipments();
  });
  document.addEventListener("click", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    if (!target.closest(".actions-wrap")) {
      document.querySelectorAll(".actions-menu").forEach((menu) => {
        menu.classList.add("hidden");
        menu.style.top = "-9999px";
        menu.style.left = "-9999px";
      });
    }
    if (target.closest("[data-modal-close='1']")) {
      closeShipmentModal();
      return;
    }
    if (target.id === "shipment-modal") {
      closeShipmentModal();
    }
    if (target.closest("[data-detail-close='1']")) {
      closeDetailModal();
      return;
    }
    if (target.closest("[data-time-close='1']")) {
      closeTimeModal();
      return;
    }
    if (target.id === "detail-modal") {
      closeDetailModal();
    }
    if (target.id === "time-modal") {
      closeTimeModal();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeShipmentModal();
      closeDetailModal();
      closeTimeModal();
    }
  });

  q("do-clear").addEventListener("click", () => {
    state.doSelected.clear();
    q("do-selected-count").textContent = "0";
    loadDOList();
  });

  q("do-download-selected").addEventListener("click", async () => {
    const shipmentIds = [...state.doSelected];
    if (!shipmentIds.length) return;
    const result = await api("/api/do-download/batch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(shipmentIds),
    });
    window.open(result.download_url, "_blank");
  });

  q("new-ticket").addEventListener("click", async () => {
    const shipmentId = prompt("Shipment ID (optional)") || "";
    const category = prompt("Category", "general") || "general";
    const description = prompt("Description", "") || "";
    await api("/api/tickets", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ shipment_id: shipmentId || null, category, description }),
    });
    await loadTickets();
  });

  q("pricing-refresh").addEventListener("click", async () => {
    await api("/api/pricing/refresh", { method: "POST" });
    await loadPricing();
    alert("Pricing refreshed");
  });
  q("overview-refresh").addEventListener("click", loadOverview);
}

async function init() {
  bindNavigation();
  bindEvents();
  try {
    if (!authToken) {
      showLogin();
      return;
    }
    currentUser = await api("/api/auth/me");
    await loadHeader();
    await Promise.all([loadOverview(), loadShipments(), loadEmptyReturn(), loadDOList(), loadPODUpload(), loadTickets(), loadPricing()]);
  } catch (_) {
    showLogin();
  }
}

init().catch((err) => {
  console.error(err);
  showAppError(`Failed to load API: ${err.message}. Please start backend with 'python run.py' and open http://localhost:8000`);
});

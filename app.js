const orders = [
  { id: "DRY-78412", customer: "Acme Imports", container: "MSKU-22094", pickup: "Long Beach", dropoff: "Ontario", eta: "09:30", status: "Queued" },
  { id: "DRY-78419", customer: "Westline Foods", container: "TGHU-88912", pickup: "San Pedro", dropoff: "Riverside", eta: "10:05", status: "In Transit" },
  { id: "DRY-78420", customer: "Pacific Retail", container: "CSNU-11209", pickup: "Long Beach", dropoff: "Corona", eta: "10:20", status: "Delayed" },
  { id: "DRY-78426", customer: "Bayline Group", container: "OOLU-67344", pickup: "Port Hueneme", dropoff: "Irvine", eta: "11:10", status: "Completed" },
  { id: "DRY-78431", customer: "Aria Trade Co.", container: "GLDU-66751", pickup: "San Diego", dropoff: "Chino", eta: "11:35", status: "Queued" },
  { id: "DRY-78433", customer: "Atlas Freight", container: "MEDU-21451", pickup: "Long Beach", dropoff: "City of Industry", eta: "12:00", status: "In Transit" },
  { id: "DRY-78435", customer: "Prime Cartage", container: "HMMU-99315", pickup: "San Pedro", dropoff: "Fontana", eta: "12:40", status: "Queued" },
  { id: "DRY-78437", customer: "Silver Harbor", container: "SEGU-77105", pickup: "Long Beach", dropoff: "Santa Ana", eta: "13:10", status: "Delayed" },
  { id: "DRY-78442", customer: "Nexa Global", container: "EMCU-14122", pickup: "San Diego", dropoff: "Pomona", eta: "13:40", status: "Completed" },
  { id: "DRY-78449", customer: "Portline USA", container: "TRHU-83990", pickup: "Long Beach", dropoff: "Anaheim", eta: "14:00", status: "In Transit" },
  { id: "DRY-78455", customer: "Kite Logistics", container: "MSCU-77452", pickup: "Port Hueneme", dropoff: "Ontario", eta: "14:30", status: "Queued" },
  { id: "DRY-78462", customer: "Urban Supply", container: "BMOU-66521", pickup: "San Pedro", dropoff: "Rialto", eta: "15:10", status: "Completed" }
];

const pageSize = 8;
let page = 1;

const body = document.getElementById("orders-body");
const searchInput = document.getElementById("search-input");
const statusFilter = document.getElementById("status-filter");
const pageLabel = document.getElementById("page-label");
const prevBtn = document.getElementById("prev-page");
const nextBtn = document.getElementById("next-page");

function updateStats(items) {
  document.getElementById("active-count").textContent = items.filter(o => o.status !== "Completed").length;
  document.getElementById("transit-count").textContent = items.filter(o => o.status === "In Transit").length;
  document.getElementById("delayed-count").textContent = items.filter(o => o.status === "Delayed").length;
  document.getElementById("completed-count").textContent = items.filter(o => o.status === "Completed").length;
}

function filteredOrders() {
  const q = searchInput.value.trim().toLowerCase();
  const f = statusFilter.value;
  return orders.filter((o) => {
    const text = `${o.id} ${o.customer} ${o.container} ${o.pickup} ${o.dropoff}`.toLowerCase();
    const matchSearch = q === "" || text.includes(q);
    const matchStatus = f === "all" || o.status === f;
    return matchSearch && matchStatus;
  });
}

function render() {
  const list = filteredOrders();
  const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
  if (page > totalPages) page = totalPages;
  if (page < 1) page = 1;

  const start = (page - 1) * pageSize;
  const pageItems = list.slice(start, start + pageSize);

  body.innerHTML = pageItems.map((o) => {
    const klass = o.status.replace(" ", "-");
    return `
      <tr>
        <td>${o.id}</td>
        <td>${o.customer}</td>
        <td>${o.container}</td>
        <td>${o.pickup}</td>
        <td>${o.dropoff}</td>
        <td>${o.eta}</td>
        <td><span class="status ${klass}">${o.status}</span></td>
      </tr>
    `;
  }).join("");

  pageLabel.textContent = `Page ${page} / ${totalPages}`;
  prevBtn.disabled = page === 1;
  nextBtn.disabled = page === totalPages;
  updateStats(list);
}

searchInput.addEventListener("input", () => {
  page = 1;
  render();
});

statusFilter.addEventListener("change", () => {
  page = 1;
  render();
});

prevBtn.addEventListener("click", () => {
  page -= 1;
  render();
});

nextBtn.addEventListener("click", () => {
  page += 1;
  render();
});

render();

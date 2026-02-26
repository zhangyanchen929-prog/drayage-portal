PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE,
  timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles'
);

CREATE TABLE IF NOT EXISTS auth_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  token TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES auth_users(id)
);

CREATE INDEX IF NOT EXISTS idx_auth_sessions_token ON auth_sessions(token);

CREATE TABLE IF NOT EXISTS shipments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_id TEXT NOT NULL UNIQUE,
  container_no TEXT NOT NULL,
  mbol TEXT,
  size TEXT,
  terminal TEXT,
  carrier TEXT,
  eta_at TEXT,
  lfd_at TEXT,
  dg INTEGER NOT NULL DEFAULT 0,
  deliver_company TEXT,
  deliver_to TEXT,
  warehouse_contact TEXT,
  warehouse_phone TEXT,
  remark TEXT,
  pickup_appt_at TEXT,
  scheduled_delivery_at TEXT,
  actual_delivery_at TEXT,
  empty_date_at TEXT,
  empty_return_at TEXT,
  waiting_port_minutes INTEGER NOT NULL DEFAULT 0,
  waiting_local_minutes INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shipments_container_no ON shipments(container_no);
CREATE INDEX IF NOT EXISTS idx_shipments_mbol ON shipments(mbol);
CREATE INDEX IF NOT EXISTS idx_shipments_terminal ON shipments(terminal);
CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_lfd ON shipments(lfd_at);
CREATE INDEX IF NOT EXISTS idx_shipments_eta ON shipments(eta_at);
CREATE INDEX IF NOT EXISTS idx_shipments_pickup ON shipments(pickup_appt_at);
CREATE INDEX IF NOT EXISTS idx_shipments_created_at ON shipments(created_at DESC);

CREATE TABLE IF NOT EXISTS shipment_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_id INTEGER NOT NULL,
  doc_type TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  verify_status TEXT NOT NULL DEFAULT 'uploaded',
  downloaded INTEGER NOT NULL DEFAULT 0,
  is_latest INTEGER NOT NULL DEFAULT 1,
  uploaded_at TEXT NOT NULL,
  FOREIGN KEY (shipment_id) REFERENCES shipments(id)
);

CREATE INDEX IF NOT EXISTS idx_shipment_documents_sid_type ON shipment_documents(shipment_id, doc_type, is_latest);

CREATE TABLE IF NOT EXISTS shipment_status_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_id INTEGER NOT NULL,
  from_status TEXT,
  to_status TEXT NOT NULL,
  note TEXT,
  changed_at TEXT NOT NULL,
  FOREIGN KEY (shipment_id) REFERENCES shipments(id)
);

CREATE INDEX IF NOT EXISTS idx_shipment_status_history_sid ON shipment_status_history(shipment_id, changed_at DESC);

CREATE TABLE IF NOT EXISTS tickets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_no TEXT NOT NULL UNIQUE,
  shipment_id INTEGER,
  category TEXT NOT NULL,
  attachment_name TEXT,
  attachment_path TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  description TEXT,
  FOREIGN KEY (shipment_id) REFERENCES shipments(id)
);

CREATE INDEX IF NOT EXISTS idx_tickets_status_created ON tickets(status, created_at DESC);

CREATE TABLE IF NOT EXISTS pricing_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  priority INTEGER NOT NULL,
  code TEXT NOT NULL,
  label TEXT NOT NULL,
  calculator TEXT NOT NULL,
  amount REAL NOT NULL,
  zone TEXT,
  container TEXT,
  free_days INTEGER,
  free_hours INTEGER,
  bill_to TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pricing_rules_priority ON pricing_rules(priority);
CREATE INDEX IF NOT EXISTS idx_pricing_rules_code ON pricing_rules(code);

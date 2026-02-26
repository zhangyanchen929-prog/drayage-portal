from __future__ import annotations

from datetime import datetime
import hashlib

from backend.db import get_conn, init_db


SHIPMENTS = [
    ("SHP-1001", "MSKU1234567", "MBL-98001", "40HC", "LBCT", "ZIM", "2026-02-25 09:00", "2026-02-27 17:00", 0, "Acme Inc", "Ontario, CA", "Liam", "6261112233", "Handle with care", "2026-02-25 10:30", "2026-02-25 14:00", None, None, None, 20, 0, "awaiting_dispatch"),
    ("SHP-1002", "TGHU2221001", "MBL-98002", "20GP", "APM", "MAERSK", "2026-02-25 08:30", "2026-02-26 16:00", 0, "Westline", "Riverside, CA", "Ava", "6261112234", "", "2026-02-25 11:00", "2026-02-25 15:30", None, None, None, 0, 10, "pending"),
    ("SHP-1003", "CSNU7610022", "MBL-98003", "40HC", "TTI", "COSCO", "2026-02-25 12:00", "2026-02-28 18:00", 0, "Pacific Retail", "Corona, CA", "Noah", "6261112235", "", "2026-02-26 08:30", "2026-02-26 12:00", None, None, None, 0, 0, "scheduled"),
    ("SHP-1004", "EMCU7755101", "MBL-98004", "40HC", "LBCT", "CMA", "2026-02-25 07:30", "2026-02-26 12:00", 1, "Nexa", "Pomona, CA", "Sophia", "6261112236", "Exam requested", "2026-02-25 13:00", "2026-02-26 10:00", None, None, None, 40, 5, "exam_on_hold"),
    ("SHP-1005", "SEGU2209384", "MBL-98005", "40HC", "YTI", "HAPAG", "2026-02-24 21:00", "2026-02-26 17:00", 0, "Atlas", "Irvine, CA", "Mia", "6261112237", "", "2026-02-25 09:30", "2026-02-25 13:30", None, None, None, 0, 0, "pre_pull"),
    ("SHP-1006", "TRHU6632100", "MBL-98006", "20GP", "APM", "ONE", "2026-02-25 06:50", "2026-02-25 20:00", 0, "Portline", "Anaheim, CA", "James", "6261112238", "", "2026-02-25 08:00", "2026-02-25 11:30", None, None, None, 0, 0, "dispatched"),
    ("SHP-1007", "MEDU8399012", "MBL-98007", "40HC", "LBCT", "MSC", "2026-02-24 09:00", "2026-02-25 17:00", 0, "Urban", "Chino, CA", "Lucas", "6261112239", "", "2026-02-24 13:00", "2026-02-24 18:00", "2026-02-24 18:20", None, None, 10, 15, "delivered"),
    ("SHP-1008", "OOLU1122098", "MBL-98008", "40HC", "TTI", "OOCL", "2026-02-24 08:20", "2026-02-25 16:00", 0, "Bayline", "Fontana, CA", "Ella", "6261112240", "", "2026-02-24 09:30", "2026-02-24 14:30", "2026-02-24 14:40", "2026-02-25 10:00", None, 0, 0, "empty_date_confirmed"),
    ("SHP-1009", "HMMU5588770", "MBL-98009", "20GP", "YTI", "HMM", "2026-02-23 08:00", "2026-02-24 17:00", 0, "Prime", "Rialto, CA", "Mason", "6261112241", "", "2026-02-23 10:00", "2026-02-23 15:30", "2026-02-23 16:10", "2026-02-24 11:30", "2026-02-25 09:20", 0, 0, "empty_returned"),
    ("SHP-1010", "ZIMU7722111", "MBL-98010", "40HC", "LBCT", "ZIM", "2026-02-22 06:00", "2026-02-23 17:00", 0, "Kite", "Santa Ana, CA", "Amelia", "6261112242", "", "2026-02-22 08:30", "2026-02-22 12:30", "2026-02-22 12:40", "2026-02-23 11:00", "2026-02-24 10:05", 0, 0, "closed"),
]

PRICING = [
    (1, "BASE_TRUCKING", "Base Trucking", "flat", 460, "LA", "20/40", 0, 0, "SHIPPER"),
    (2, "PIER_PASS_AM", "Pier Pass AM", "flat", 40, "LA", "ALL", 0, 0, "SHIPPER"),
    (3, "CTF_STANDARD", "CTF Standard", "flat", 35, "LA", "ALL", 0, 0, "SHIPPER"),
    (4, "WAITING_PORT", "Waiting Port", "per_hour", 95, "LA", "ALL", 0, 1, "CONSIGNEE"),
    (5, "WAITING_LOCAL", "Waiting Local", "per_hour", 85, "LA", "ALL", 0, 1, "CONSIGNEE"),
    (6, "OVERWEIGHT_20", "Overweight 20", "flat", 160, "LA", "20GP", 0, 0, "SHIPPER"),
    (7, "HAZMAT", "Hazmat", "flat", 120, "LA", "ALL", 0, 0, "SHIPPER"),
    (8, "YARD_STORAGE", "Yard Storage", "per_day", 45, "LA", "ALL", 2, 0, "CONSIGNEE"),
]


def seed_data() -> None:
    init_db()
    now = datetime.utcnow().isoformat(timespec="seconds")

    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO users(email, timezone) VALUES(?, ?)", ("ops@example.com", "America/Los_Angeles"))
        auth_users = [
            ("customer@demo.com", hashlib.sha256("customer123".encode("utf-8")).hexdigest(), "customer", "America/Los_Angeles"),
            ("operator@demo.com", hashlib.sha256("operator123".encode("utf-8")).hexdigest(), "operator", "America/Los_Angeles"),
        ]
        for email, password_hash, role, timezone in auth_users:
            conn.execute(
                """
                INSERT OR IGNORE INTO auth_users(email, password_hash, role, timezone, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (email, password_hash, role, timezone, now),
            )

        existing = conn.execute("SELECT COUNT(*) c FROM shipments").fetchone()["c"]
        if existing == 0:
            for row in SHIPMENTS:
                conn.execute(
                    """
                    INSERT INTO shipments(
                      shipment_id, container_no, mbol, size, terminal, carrier, eta_at, lfd_at, dg,
                      deliver_company, deliver_to, warehouse_contact, warehouse_phone, remark,
                      pickup_appt_at, scheduled_delivery_at, actual_delivery_at, empty_date_at, empty_return_at,
                      waiting_port_minutes, waiting_local_minutes, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (*row, now, now),
                )

        existing_pricing = conn.execute("SELECT COUNT(*) c FROM pricing_rules").fetchone()["c"]
        if existing_pricing == 0:
            for p in PRICING:
                conn.execute(
                    """
                    INSERT INTO pricing_rules(priority, code, label, calculator, amount, zone, container, free_days, free_hours, bill_to, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (*p, now),
                )

        conn.commit()


def main() -> None:
    seed_data()


if __name__ == "__main__":
    main()

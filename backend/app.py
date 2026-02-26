from __future__ import annotations

import sqlite3
import zipfile
from datetime import datetime
from datetime import timedelta
import hashlib
from pathlib import Path
import secrets
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.db import UPLOAD_DIR, get_conn, init_db
from backend.seed import seed_data

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
DOWNLOADS_DIR = BASE_DIR / "downloads"
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

STATUS_ORDER = [
    "awaiting_dispatch",
    "pending",
    "scheduled",
    "exam_on_hold",
    "pre_pull",
    "dispatched",
    "delivered",
    "empty_date_confirmed",
    "empty_returned",
    "closed",
]

DISPLAY_STATUS = {
    "awaiting_dispatch": "Awaiting Dispatch",
    "pending": "Pending",
    "scheduled": "Scheduled",
    "exam_on_hold": "Exam/On Hold",
    "pre_pull": "Pre-pull",
    "dispatched": "Dispatched",
    "delivered": "Delivered",
    "empty_date_confirmed": "Empty Date Confirmed",
    "empty_returned": "Empty Returned",
    "closed": "Closed",
}


class TicketCreate(BaseModel):
    shipment_id: str | None = None
    category: str
    description: str = ""


class StatusUpdate(BaseModel):
    to_status: str
    note: str = ""


class ShipmentTimeUpdate(BaseModel):
    pickup_appt_at: str = ""


class ShipmentCreate(BaseModel):
    shipment_id: str
    container_no: str
    mbol: str = ""
    size: str = "40HC"
    terminal: str = ""
    carrier: str = ""
    eta_at: str = ""
    lfd_at: str = ""
    dg: bool = False
    deliver_company: str = ""
    deliver_to: str = ""
    warehouse_contact: str = ""
    warehouse_phone: str = ""
    remark: str = ""
    pickup_appt_at: str = ""
    scheduled_delivery_at: str = ""
    status: str = "awaiting_dispatch"


class LoginRequest(BaseModel):
    email: str
    password: str


app = FastAPI(title="Drayage Portal API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    init_db()
    seed_data()


def password_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def require_user(request: Request) -> dict[str, Any]:
    user = getattr(request.state, "current_user", None)
    if not user:
        raise HTTPException(401, "Unauthorized")
    return user


def require_role(request: Request, roles: set[str]) -> dict[str, Any]:
    user = require_user(request)
    if user["role"] not in roles:
        raise HTTPException(403, "Forbidden")
    return user


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api"):
        return await call_next(request)
    if path == "/api/auth/login":
        return await call_next(request)

    auth_header = request.headers.get("authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    if not token:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.email, u.role, u.timezone
            FROM auth_sessions s
            JOIN auth_users u ON u.id = s.user_id
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, datetime.utcnow().isoformat(timespec="seconds")),
        ).fetchone()
        if not row:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
        request.state.current_user = dict(row)
        request.state.current_token = token

    return await call_next(request)


def row_to_shipment(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["status_label"] = DISPLAY_STATUS.get(data["status"], data["status"])
    return data


def get_latest_document(conn: sqlite3.Connection, shipment_pk: int, doc_type: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, file_name, verify_status, downloaded, uploaded_at
        FROM shipment_documents
        WHERE shipment_id = ? AND doc_type = ? AND is_latest = 1
        ORDER BY id DESC LIMIT 1
        """,
        (shipment_pk, doc_type),
    ).fetchone()
    return dict(row) if row else None


@app.post("/api/auth/login")
def login(payload: LoginRequest) -> dict[str, Any]:
    with get_conn() as conn:
        user = conn.execute(
            "SELECT id, email, role, timezone, password_hash FROM auth_users WHERE email = ?",
            (payload.email.strip().lower(),),
        ).fetchone()
        if not user or user["password_hash"] != password_hash(payload.password):
            raise HTTPException(401, "Invalid credentials")

        token = secrets.token_urlsafe(32)
        now = datetime.utcnow()
        expires_at = (now + timedelta(hours=12)).isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO auth_sessions(user_id, token, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (user["id"], token, now.isoformat(timespec="seconds"), expires_at),
        )
        conn.commit()
        return {
            "token": token,
            "user": {
                "email": user["email"],
                "role": user["role"],
                "timezone": user["timezone"],
            },
        }


@app.get("/api/auth/me")
def me(request: Request) -> dict[str, Any]:
    user = require_user(request)
    return {
        "email": user["email"],
        "role": user["role"],
        "timezone": user["timezone"],
    }


@app.post("/api/auth/logout")
def logout(request: Request) -> dict[str, Any]:
    token = getattr(request.state, "current_token", None)
    if token:
        with get_conn() as conn:
            conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
            conn.commit()
    return {"ok": True}


@app.get("/api/overview/stats")
def overview_stats() -> dict[str, Any]:
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM shipments").fetchone()["c"]
        stats = {"total": total}
        for s in STATUS_ORDER:
            stats[s] = conn.execute("SELECT COUNT(*) c FROM shipments WHERE status = ?", (s,)).fetchone()["c"]
        return stats


@app.get("/api/overview/shipments")
def overview_shipments(search: str = "", status: str = "all", page: int = 1, page_size: int = 20) -> dict[str, Any]:
    with get_conn() as conn:
        where = ["1=1"]
        args: list[Any] = []
        if search:
            where.append("(shipment_id LIKE ? OR container_no LIKE ? OR mbol LIKE ? OR terminal LIKE ?)")
            like = f"%{search}%"
            args.extend([like, like, like, like])
        if status != "all":
            where.append("status = ?")
            args.append(status)

        where_sql = " AND ".join(where)
        total = conn.execute(f"SELECT COUNT(*) c FROM shipments WHERE {where_sql}", args).fetchone()["c"]
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT shipment_id, container_no, eta_at, lfd_at, status
            FROM shipments
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            [*args, page_size, offset],
        ).fetchall()

        return {
            "items": [
                {
                    "shipment_id": r["shipment_id"],
                    "container_no": r["container_no"],
                    "eta_at": r["eta_at"],
                    "lfd_at": r["lfd_at"],
                    "status": r["status"],
                    "status_label": DISPLAY_STATUS.get(r["status"], r["status"]),
                }
                for r in rows
            ],
            "page": page,
            "page_size": page_size,
            "total": total,
        }


@app.get("/api/shipments")
def list_shipments(
    search: str = "",
    status: str = "all",
    today_pickup: bool = False,
    next_day_pickup: bool = False,
    pre_pull_only: bool = False,
    sort: str = "created_desc",
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    sort_map = {
        "status_priority": "CASE status " + " ".join([f"WHEN '{s}' THEN {i}" for i, s in enumerate(STATUS_ORDER)]) + " END ASC",
        "lfd_asc": "lfd_at ASC",
        "pu_appt_asc": "pickup_appt_at ASC",
        "sch_del_asc": "scheduled_delivery_at ASC",
        "eta_asc": "eta_at ASC",
        "empty_ret_asc": "empty_return_at ASC",
        "created_desc": "created_at DESC",
        "terminal_az": "terminal ASC",
    }
    order_by = sort_map.get(sort, "created_at DESC")

    where = ["1=1"]
    args: list[Any] = []

    if search:
        like = f"%{search}%"
        where.append(
            "(shipment_id LIKE ? OR container_no LIKE ? OR mbol LIKE ? OR terminal LIKE ? OR deliver_to LIKE ? OR status LIKE ?)"
        )
        args.extend([like, like, like, like, like, like])
    if status != "all":
        where.append("status = ?")
        args.append(status)

    now = datetime.now()
    if today_pickup:
        where.append("date(pickup_appt_at) = date(?)")
        args.append(now.isoformat(sep=" "))
    if next_day_pickup:
        where.append("date(pickup_appt_at) = date(?, '+1 day')")
        args.append(now.isoformat(sep=" "))
    if pre_pull_only:
        where.append("status = 'pre_pull'")

    where_sql = " AND ".join(where)

    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) c FROM shipments WHERE {where_sql}", args).fetchone()["c"]
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"SELECT * FROM shipments WHERE {where_sql} ORDER BY {order_by} LIMIT ? OFFSET ?",
            [*args, page_size, offset],
        ).fetchall()

        items = []
        for row in rows:
            shipment = row_to_shipment(row)
            shipment["pod"] = get_latest_document(conn, row["id"], "POD")
            shipment["do"] = get_latest_document(conn, row["id"], "DO")
            items.append(shipment)

        return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/api/shipments")
def create_shipment(payload: ShipmentCreate) -> dict[str, Any]:
    if payload.status not in STATUS_ORDER:
        raise HTTPException(400, "Invalid status")

    def val(v: str) -> str | None:
        s = v.strip()
        return s if s else None

    now = datetime.utcnow().isoformat(timespec="seconds")
    with get_conn() as conn:
        try:
            conn.execute(
                """
                INSERT INTO shipments(
                  shipment_id, container_no, mbol, size, terminal, carrier, eta_at, lfd_at, dg,
                  deliver_company, deliver_to, warehouse_contact, warehouse_phone, remark,
                  pickup_appt_at, scheduled_delivery_at, actual_delivery_at, empty_date_at, empty_return_at,
                  waiting_port_minutes, waiting_local_minutes, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, 0, ?, ?, ?)
                """,
                (
                    payload.shipment_id.strip(),
                    payload.container_no.strip(),
                    val(payload.mbol),
                    payload.size.strip() or "40HC",
                    val(payload.terminal),
                    val(payload.carrier),
                    val(payload.eta_at),
                    val(payload.lfd_at),
                    1 if payload.dg else 0,
                    val(payload.deliver_company),
                    val(payload.deliver_to),
                    val(payload.warehouse_contact),
                    val(payload.warehouse_phone),
                    val(payload.remark),
                    val(payload.pickup_appt_at),
                    val(payload.scheduled_delivery_at),
                    payload.status,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Shipment ID already exists")

        row = conn.execute("SELECT * FROM shipments WHERE shipment_id = ?", (payload.shipment_id.strip(),)).fetchone()
        conn.commit()
        return {"ok": True, "item": row_to_shipment(row)}


@app.get("/api/shipments/{shipment_id}")
def shipment_detail(shipment_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        shipment = row_to_shipment(row)
        shipment["documents"] = {
            "DO": get_latest_document(conn, row["id"], "DO"),
            "POD": get_latest_document(conn, row["id"], "POD"),
        }
        shipment["timeline"] = {
            "pickup": shipment.get("pickup_appt_at"),
            "delivery_scheduled": shipment.get("scheduled_delivery_at"),
            "delivery_actual": shipment.get("actual_delivery_at"),
            "empty_return": shipment.get("empty_return_at"),
            "empty_date": shipment.get("empty_date_at"),
        }
        return shipment


@app.post("/api/shipments/{shipment_id}/status")
def update_status(shipment_id: str, payload: StatusUpdate, request: Request) -> dict[str, Any]:
    require_role(request, {"operator"})
    if payload.to_status not in STATUS_ORDER:
        raise HTTPException(400, "Invalid status")

    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, status, actual_delivery_at, empty_date_at, empty_return_at FROM shipments WHERE shipment_id = ?",
            (shipment_id,),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")

        now_ts = datetime.utcnow().isoformat(timespec="seconds")
        extra_sets: list[str] = []
        extra_vals: list[Any] = []
        if payload.to_status in {"delivered", "empty_date_confirmed", "empty_returned", "closed"} and not row["actual_delivery_at"]:
            extra_sets.append("actual_delivery_at = ?")
            extra_vals.append(now_ts)
        if payload.to_status in {"empty_date_confirmed", "empty_returned", "closed"} and not row["empty_date_at"]:
            extra_sets.append("empty_date_at = ?")
            extra_vals.append(now_ts)
        if payload.to_status in {"empty_returned", "closed"} and not row["empty_return_at"]:
            extra_sets.append("empty_return_at = ?")
            extra_vals.append(now_ts)

        update_sql = "UPDATE shipments SET status = ?, updated_at = ?"
        if extra_sets:
            update_sql += ", " + ", ".join(extra_sets)
        update_sql += " WHERE shipment_id = ?"

        conn.execute(
            update_sql,
            [payload.to_status, now_ts, *extra_vals, shipment_id],
        )
        conn.execute(
            """
            INSERT INTO shipment_status_history(shipment_id, from_status, to_status, note, changed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (row["id"], row["status"], payload.to_status, payload.note, now_ts),
        )
        conn.commit()
        return {"ok": True}


@app.post("/api/shipments/{shipment_id}/times")
def update_shipment_times(shipment_id: str, payload: ShipmentTimeUpdate, request: Request) -> dict[str, Any]:
    require_role(request, {"operator"})
    pu_appt = payload.pickup_appt_at.strip() or None
    with get_conn() as conn:
        row = conn.execute("SELECT id, status, pickup_appt_at FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        now_ts = datetime.utcnow().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE shipments
            SET pickup_appt_at = ?, scheduled_delivery_at = ?, actual_delivery_at = ?, updated_at = ?
            WHERE shipment_id = ?
            """,
            (pu_appt, pu_appt, pu_appt, now_ts, shipment_id),
        )
        # Auto-flow: once PU appointment is set by operator, awaiting/pending shipments move to Scheduled.
        if pu_appt and row["status"] in {"awaiting_dispatch", "pending"}:
            conn.execute(
                "UPDATE shipments SET status = ?, updated_at = ? WHERE shipment_id = ?",
                ("scheduled", now_ts, shipment_id),
            )
            conn.execute(
                """
                INSERT INTO shipment_status_history(shipment_id, from_status, to_status, note, changed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (row["id"], row["status"], "scheduled", "Auto update after PU Appt set", now_ts),
            )
        conn.commit()
    return {"ok": True}


@app.delete("/api/shipments/{shipment_id}")
def delete_shipment(shipment_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        shipment_pk = row["id"]

        conn.execute("UPDATE tickets SET shipment_id = NULL WHERE shipment_id = ?", (shipment_pk,))
        conn.execute("DELETE FROM shipment_documents WHERE shipment_id = ?", (shipment_pk,))
        conn.execute("DELETE FROM shipment_status_history WHERE shipment_id = ?", (shipment_pk,))
        conn.execute("DELETE FROM shipments WHERE id = ?", (shipment_pk,))
        conn.commit()
        return {"ok": True}


@app.post("/api/shipments/{shipment_id}/documents/{doc_type}")
async def upload_document(shipment_id: str, doc_type: str, request: Request, file: UploadFile = File(...)) -> dict[str, Any]:
    dtype = doc_type.upper()
    if dtype not in {"DO", "POD"}:
        raise HTTPException(400, "doc_type must be DO or POD")
    user = require_user(request)
    if dtype == "DO" and user["role"] != "customer":
        raise HTTPException(403, "Only customer can upload DO")
    if dtype == "POD" and user["role"] != "operator":
        raise HTTPException(403, "Only operator can upload POD")

    with get_conn() as conn:
        row = conn.execute("SELECT id FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        shipment_pk = row["id"]

        conn.execute(
            "UPDATE shipment_documents SET is_latest = 0 WHERE shipment_id = ? AND doc_type = ?",
            (shipment_pk, dtype),
        )

        suffix = Path(file.filename).suffix
        safe_name = f"{shipment_id}_{dtype}_{int(datetime.utcnow().timestamp())}{suffix}"
        file_path = UPLOAD_DIR / safe_name
        content = await file.read()
        file_path.write_bytes(content)

        conn.execute(
            """
            INSERT INTO shipment_documents(shipment_id, doc_type, file_name, file_path, verify_status, downloaded, is_latest, uploaded_at)
            VALUES (?, ?, ?, ?, 'uploaded', 0, 1, ?)
            """,
            (shipment_pk, dtype, file.filename, str(file_path), datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()

    return {"ok": True, "file_name": file.filename}


@app.get("/api/documents/{doc_id}/download")
def download_document(doc_id: int) -> FileResponse:
    with get_conn() as conn:
        doc = conn.execute("SELECT * FROM shipment_documents WHERE id = ?", (doc_id,)).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        file_path = Path(doc["file_path"])
        if not file_path.exists():
            raise HTTPException(404, "File missing")

        conn.execute("UPDATE shipment_documents SET downloaded = 1 WHERE id = ?", (doc_id,))
        conn.commit()

        return FileResponse(path=file_path, filename=doc["file_name"], media_type="application/octet-stream")


@app.get("/api/empty-returns")
def empty_returns(search: str = "", page: int = 1, page_size: int = 20) -> dict[str, Any]:
    statuses = ("delivered", "empty_date_confirmed", "empty_returned")
    where = ["status IN (?, ?, ?)"]
    args: list[Any] = list(statuses)
    if search:
        like = f"%{search}%"
        where.append("(shipment_id LIKE ? OR container_no LIKE ? OR mbol LIKE ? OR terminal LIKE ?)")
        args.extend([like, like, like, like])

    where_sql = " AND ".join(where)
    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) c FROM shipments WHERE {where_sql}", args).fetchone()["c"]
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT shipment_id, mbol, container_no, terminal, deliver_to,
                   actual_delivery_at, empty_date_at, empty_return_at, status
            FROM shipments
            WHERE {where_sql}
            ORDER BY actual_delivery_at DESC
            LIMIT ? OFFSET ?
            """,
            [*args, page_size, offset],
        ).fetchall()
        return {
            "items": [
                {
                    "shipment_id": r["shipment_id"],
                    "mbol": r["mbol"],
                    "container_no": r["container_no"],
                    "terminal": r["terminal"],
                    "deliver_to": r["deliver_to"],
                    "delivered": r["actual_delivery_at"],
                    "empty_date": r["empty_date_at"],
                    "empty_returned": r["empty_return_at"],
                    "status": r["status"],
                    "status_label": DISPLAY_STATUS.get(r["status"], r["status"]),
                }
                for r in rows
            ],
            "total": total,
        }


@app.get("/api/do-download/list")
def do_download_list(search: str = "") -> dict[str, Any]:
    with get_conn() as conn:
        where = ["1=1"]
        args: list[Any] = []
        if search:
            like = f"%{search}%"
            where.append("(s.shipment_id LIKE ? OR s.container_no LIKE ? OR s.deliver_to LIKE ? OR d.file_name LIKE ?)")
            args.extend([like, like, like, like])
        rows = conn.execute(
            f"""
            SELECT s.shipment_id, s.container_no, s.deliver_to,
                   d.id as doc_id, d.file_name, d.downloaded
            FROM shipments s
            JOIN shipment_documents d ON d.shipment_id = s.id AND d.doc_type = 'DO' AND d.is_latest = 1
            WHERE {' AND '.join(where)}
            ORDER BY s.created_at DESC
            """,
            args,
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.post("/api/do-download/batch")
def do_batch_download(shipment_ids: list[str]) -> dict[str, Any]:
    if not shipment_ids:
        raise HTTPException(400, "No shipments selected")

    with get_conn() as conn:
        placeholders = ",".join("?" for _ in shipment_ids)
        rows = conn.execute(
            f"""
            SELECT d.id as doc_id, d.file_path, d.file_name, s.shipment_id
            FROM shipment_documents d
            JOIN shipments s ON s.id = d.shipment_id
            WHERE s.shipment_id IN ({placeholders}) AND d.doc_type = 'DO' AND d.is_latest = 1
            """,
            shipment_ids,
        ).fetchall()

        if not rows:
            raise HTTPException(404, "No DO files found")

        zip_name = f"do_batch_{int(datetime.utcnow().timestamp())}.zip"
        zip_path = DOWNLOADS_DIR / zip_name
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for row in rows:
                fp = Path(row["file_path"])
                if fp.exists():
                    zf.write(fp, arcname=f"{row['shipment_id']}_{row['file_name']}")
                    conn.execute("UPDATE shipment_documents SET downloaded = 1 WHERE id = ?", (row["doc_id"],))
        conn.commit()

        return {"download_url": f"/api/downloads/{zip_name}"}


@app.get("/api/downloads/{file_name}")
def download_batch_zip(file_name: str) -> FileResponse:
    fp = DOWNLOADS_DIR / file_name
    if not fp.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(fp, filename=file_name, media_type="application/zip")


@app.get("/api/pod-upload/list")
def pod_upload_list(search: str = "") -> dict[str, Any]:
    where = ["s.status = 'dispatched'"]
    args: list[Any] = []
    if search:
        like = f"%{search}%"
        where.append("(s.shipment_id LIKE ? OR s.container_no LIKE ? OR s.terminal LIKE ? OR s.deliver_to LIKE ? OR s.status LIKE ?)")
        args.extend([like, like, like, like, like])

    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT s.shipment_id, s.container_no, s.mbol, s.terminal, s.eta_at, s.lfd_at,
                   d.id as pod_doc_id, d.file_name as pod_file_name, d.verify_status
            FROM shipments s
            LEFT JOIN shipment_documents d ON d.shipment_id = s.id AND d.doc_type = 'POD' AND d.is_latest = 1
            WHERE {' AND '.join(where)}
            ORDER BY s.created_at DESC
            """,
            args,
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/tickets")
def list_tickets() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT t.ticket_no, s.shipment_id, t.category, t.attachment_name, t.status, t.created_at
            FROM tickets t
            LEFT JOIN shipments s ON s.id = t.shipment_id
            ORDER BY t.created_at DESC
            """
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.post("/api/tickets")
def create_ticket(payload: TicketCreate) -> dict[str, Any]:
    with get_conn() as conn:
        ticket_no = f"T{int(datetime.utcnow().timestamp())}"
        shipment_pk = None
        if payload.shipment_id:
            row = conn.execute("SELECT id FROM shipments WHERE shipment_id = ?", (payload.shipment_id,)).fetchone()
            if row:
                shipment_pk = row["id"]
        conn.execute(
            """
            INSERT INTO tickets(ticket_no, shipment_id, category, status, created_at, description)
            VALUES (?, ?, ?, 'open', ?, ?)
            """,
            (ticket_no, shipment_pk, payload.category, datetime.utcnow().isoformat(timespec="seconds"), payload.description),
        )
        conn.commit()
    return {"ok": True, "ticket_no": ticket_no}


@app.get("/api/pricing/rules")
def pricing_rules() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT priority, code, label, calculator, amount, zone, container, free_days, free_hours, bill_to
            FROM pricing_rules
            WHERE is_active = 1
            ORDER BY priority ASC
            """
        ).fetchall()
        return {"items": [dict(r) for r in rows], "count": len(rows)}


@app.post("/api/pricing/refresh")
def pricing_refresh(request: Request) -> dict[str, Any]:
    require_role(request, {"operator"})
    return {"ok": True, "refreshed_at": datetime.utcnow().isoformat(timespec="seconds")}


app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

from __future__ import annotations

import sqlite3
import zipfile
import base64
import io
import imaplib
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from datetime import datetime
from datetime import timedelta
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from fastapi import FastAPI, File, HTTPException, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.db import UPLOAD_DIR, get_conn, init_db
from backend.email_extractor import anthropic_extract
from backend.seed import seed_data
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import service_account
from pypdf import PdfReader

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

GMAIL_COOKIE_NAME = "gmail_do_sid"
GMAIL_SESSION_TTL_SECONDS = 86400
GMAIL_OAUTH_SESSIONS: dict[str, dict[str, Any]] = {}


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


class RegisterRequest(BaseModel):
    email: str
    password: str


class UserRoleUpdate(BaseModel):
    role: str


class GmailProcessRequest(BaseModel):
    message_id: str


class ImapConnectRequest(BaseModel):
    email: str
    password: str
    host: str
    port: int = 993


class SheetAppendRequest(BaseModel):
    sheet_id_or_url: str
    sheet_name: str = "Sheet1"
    attachments: list[dict[str, Any]]


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
    with get_conn() as conn:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(shipments)").fetchall()}
        if "owner_user_id" not in cols:
            conn.execute("ALTER TABLE shipments ADD COLUMN owner_user_id INTEGER")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shipments_owner_user_id ON shipments(owner_user_id)")
        conn.commit()
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


def shipment_scope(user: dict[str, Any]) -> tuple[str, list[Any]]:
    if user["role"] == "customer":
        return "owner_user_id = ?", [user["id"]]
    return "1=1", []


def gmail_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise HTTPException(500, f"Missing environment variable: {name}")
    return value or ""


def cleanup_gmail_sessions() -> None:
    now_ts = datetime.utcnow().timestamp()
    stale = [
        sid
        for sid, session in GMAIL_OAUTH_SESSIONS.items()
        if now_ts - session.get("created_at_ts", now_ts) > GMAIL_SESSION_TTL_SECONDS
    ]
    for sid in stale:
        GMAIL_OAUTH_SESSIONS.pop(sid, None)


def get_or_create_gmail_session(request: Request) -> tuple[str, dict[str, Any], bool]:
    cleanup_gmail_sessions()
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    if sid and sid in GMAIL_OAUTH_SESSIONS:
        session = GMAIL_OAUTH_SESSIONS[sid]
        return sid, session, False

    sid = secrets.token_urlsafe(24)
    session = {"created_at_ts": datetime.utcnow().timestamp()}
    GMAIL_OAUTH_SESSIONS[sid] = session
    return sid, session, True


def set_gmail_cookie(response: JSONResponse | RedirectResponse, sid: str) -> None:
    response.set_cookie(
        key=GMAIL_COOKIE_NAME,
        value=sid,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=GMAIL_SESSION_TTL_SECONDS,
    )


def google_token_request(form_data: dict[str, str]) -> dict[str, Any]:
    payload = urllib.parse.urlencode(form_data).encode("utf-8")
    req = urllib.request.Request(
        url="https://oauth2.googleapis.com/token",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(400, f"Google token exchange failed: {body}") from exc


def refresh_google_access_token(session: dict[str, Any]) -> None:
    refresh_token = session.get("refresh_token", "")
    if not refresh_token:
        raise HTTPException(401, "Google session expired. Please reconnect.")

    token_data = google_token_request(
        {
            "client_id": gmail_env("GOOGLE_CLIENT_ID", required=True),
            "client_secret": gmail_env("GOOGLE_CLIENT_SECRET", required=True),
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    )
    session["access_token"] = token_data.get("access_token", "")
    session["expires_at_ts"] = datetime.utcnow().timestamp() + int(token_data.get("expires_in", 3600)) - 30
    if not session["access_token"]:
        raise HTTPException(401, "Unable to refresh Google access token.")


def get_google_access_token(session: dict[str, Any]) -> str:
    access_token = session.get("access_token", "")
    expires_at_ts = float(session.get("expires_at_ts", 0))
    if not access_token or datetime.utcnow().timestamp() >= expires_at_ts:
        refresh_google_access_token(session)
    return session.get("access_token", "")


def gmail_api_request(
    session: dict[str, Any],
    url: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    retry_on_401: bool = True,
) -> dict[str, Any]:
    token = get_google_access_token(session)
    data_bytes = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url=url,
        method=method,
        data=data_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        if exc.code == 401 and retry_on_401:
            refresh_google_access_token(session)
            return gmail_api_request(session, url, method=method, body=body, retry_on_401=False)
        body_text = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(exc.code, f"Gmail API request failed: {body_text}") from exc


def google_api_request_with_bearer(
    token: str,
    url: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data_bytes = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url=url,
        method=method,
        data=data_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(exc.code, f"Google API request failed: {body_text}") from exc


def get_service_account_info() -> dict[str, Any]:
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(500, "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc

    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError as exc:
            raise HTTPException(500, f"GOOGLE_SERVICE_ACCOUNT_FILE not found: {file_path}") from exc
        except json.JSONDecodeError as exc:
            raise HTTPException(500, f"GOOGLE_SERVICE_ACCOUNT_FILE is not valid JSON: {file_path}") from exc

    raise HTTPException(
        400,
        "IMAP mode push requires GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE.",
    )


def get_service_account_sheets_token() -> str:
    info = get_service_account_info()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        creds.refresh(GoogleRequest())
    except Exception as exc:
        raise HTTPException(400, f"Service account auth failed: {exc}") from exc
    token = creds.token or ""
    if not token:
        raise HTTPException(400, "Failed to obtain service account access token.")
    return token


def get_gmail_profile(session: dict[str, Any]) -> dict[str, Any]:
    return gmail_api_request(session, "https://gmail.googleapis.com/gmail/v1/users/me/profile")


def collect_pdf_parts(part: dict[str, Any], out: list[dict[str, Any]]) -> None:
    mime_type = (part.get("mimeType") or "").lower()
    filename = part.get("filename") or ""
    body = part.get("body") or {}
    attachment_id = body.get("attachmentId")
    inline_data = body.get("data")
    if mime_type == "application/pdf" or filename.lower().endswith(".pdf"):
        out.append(
            {
                "filename": filename or "attachment.pdf",
                "attachment_id": attachment_id,
                "inline_data": inline_data,
            }
        )
    for child in part.get("parts") or []:
        collect_pdf_parts(child, out)


def parse_header_map(headers: list[dict[str, str]] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for header in headers or []:
        name = (header.get("name") or "").lower()
        if name:
            out[name] = header.get("value") or ""
    return out


def extract_email_address(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    m = re.search(r"<([^>]+@[^>]+)>", raw)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})", raw)
    return m2.group(1).strip() if m2 else ""


def decode_base64url(value: str) -> bytes:
    padded = value + ("=" * (-len(value) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def parse_query_newer_than_days(q: str, default_days: int = 14) -> int:
    m = re.search(r"newer_than:(\d+)d", q or "", re.IGNORECASE)
    if not m:
        return default_days
    try:
        return max(1, int(m.group(1)))
    except ValueError:
        return default_days


def parse_or_terms(fragment: str) -> list[str]:
    if not fragment:
        return []
    text = fragment.replace('"', " ").replace("(", " ").replace(")", " ")
    terms = []
    for part in re.split(r"\bOR\b|,", text, flags=re.IGNORECASE):
        t = part.strip().lower()
        if t:
            terms.append(t)
    return terms


def extract_subject_filename_terms(q: str) -> tuple[list[str], list[str]]:
    subject_terms: list[str] = []
    file_terms: list[str] = []

    m_subject = re.search(r"subject:\((.*?)\)", q or "", re.IGNORECASE)
    if m_subject:
        subject_terms.extend(parse_or_terms(m_subject.group(1)))
    m_file = re.search(r"filename:\((.*?)\)", q or "", re.IGNORECASE)
    if m_file:
        file_terms.extend(parse_or_terms(m_file.group(1)))

    return subject_terms, file_terms


def imap_config_from_session(session: dict[str, Any]) -> dict[str, Any]:
    cfg = session.get("imap_config") or {}
    host = (cfg.get("host") or "").strip()
    email_addr = (cfg.get("email") or "").strip()
    password = cfg.get("password") or ""
    port = int(cfg.get("port") or 993)
    if not host or not email_addr or not password:
        raise HTTPException(401, "Not connected to IMAP.")
    return {"host": host, "email": email_addr, "password": password, "port": port}


def imap_open(cfg: dict[str, Any]) -> imaplib.IMAP4_SSL:
    try:
        client = imaplib.IMAP4_SSL(cfg["host"], cfg["port"])
        client.login(cfg["email"], cfg["password"])
        return client
    except imaplib.IMAP4.error as exc:
        raise HTTPException(401, f"IMAP login failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(400, f"IMAP connect failed: {exc}") from exc


def collect_imap_pdf_parts(msg: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for part in msg.walk():
        filename = (part.get_filename() or "").strip()
        content_type = (part.get_content_type() or "").lower()
        if not filename and content_type != "application/pdf":
            continue
        if content_type == "application/pdf" or filename.lower().endswith(".pdf"):
            payload = part.get_payload(decode=True) or b""
            out.append(
                {
                    "filename": filename or "attachment.pdf",
                    "bytes": payload,
                }
            )
    return out


def header_to_datetime(msg: Any) -> datetime | None:
    raw = msg.get("Date", "")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt and dt.tzinfo:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def imap_fetch_message_bytes(client: imaplib.IMAP4_SSL, uid: str) -> bytes:
    typ, data = client.uid("fetch", uid, "(RFC822)")
    if typ != "OK" or not data or not data[0]:
        raise HTTPException(404, f"Message not found: {uid}")
    return data[0][1]


def extract_sheet_id(sheet_id_or_url: str) -> str:
    raw = (sheet_id_or_url or "").strip()
    if not raw:
        raise HTTPException(400, "Missing Google Sheet id or URL.")
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw)
    return m.group(1) if m else raw


def extract_sheet_gid(sheet_id_or_url: str) -> int | None:
    raw = (sheet_id_or_url or "").strip()
    if not raw:
        return None
    m = re.search(r"(?:[?#&]gid=)(\d+)", raw)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def quote_sheet_title(title: str) -> str:
    t = (title or "").replace("'", "''")
    return f"'{t}'"


def resolve_sheet_title(
    sheet_id: str,
    preferred_name: str,
    gid: int | None,
    session: dict[str, Any] | None = None,
    bearer_token: str = "",
) -> str:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}?fields=sheets(properties(sheetId,title))"
    if bearer_token:
        meta = google_api_request_with_bearer(bearer_token, url)
    elif session is not None:
        meta = gmail_api_request(session, url)
    else:
        raise HTTPException(500, "resolve_sheet_title requires session or bearer_token.")
    sheets = (meta or {}).get("sheets") or []
    titles: list[str] = []
    gid_map: dict[int, str] = {}
    for s in sheets:
        p = s.get("properties") or {}
        title = (p.get("title") or "").strip()
        sid = p.get("sheetId")
        if title:
            titles.append(title)
        if isinstance(sid, int) and title:
            gid_map[sid] = title

    if gid is not None and gid in gid_map:
        return gid_map[gid]

    preferred = (preferred_name or "").strip()
    if preferred:
        for t in titles:
            if t == preferred:
                return t
        for t in titles:
            if t.lower() == preferred.lower():
                return t

    if titles:
        return titles[0]

    raise HTTPException(400, "No worksheet tabs found in this spreadsheet.")


def get_next_sheet_row_index(
    sheet_id: str,
    sheet_title: str,
    session: dict[str, Any] | None = None,
    bearer_token: str = "",
) -> int:
    col_a_range = f"{quote_sheet_title(sheet_title)}!A:A"
    encoded_range = urllib.parse.quote(col_a_range, safe="!:")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded_range}"
    if bearer_token:
        data = google_api_request_with_bearer(bearer_token, url)
    elif session is not None:
        data = gmail_api_request(session, url)
    else:
        raise HTTPException(500, "get_next_sheet_row_index requires session or bearer_token.")
    values = (data or {}).get("values") or []
    return len(values) + 1


def to_sheet_rows(attachments: list[dict[str, Any]]) -> list[list[Any]]:
    def fmt_mmddyy(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
        if m:
            return f"{m.group(2)}/{m.group(3)}/{m.group(1)[2:]}"
        m2 = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", raw)
        if m2:
            return f"{m2.group(2)}/{m2.group(3)}/{m2.group(1)[2:]}"
        return raw

    def normalize_size(container_type: str) -> str:
        t = (container_type or "").upper()
        m = re.search(r"\b(20|40|45)\b", t)
        if m:
            return m.group(1)
        m2 = re.search(r"(20|40|45)", t)
        if m2:
            return m2.group(1)
        return container_type or ""

    rows: list[list[Any]] = []
    for att in attachments:
        result = att.get("result") or {}
        filename = att.get("filename") or ""
        do = result.get("delivery_order") or {}
        shipment = do.get("shipment") or {}
        deliver_to = do.get("deliver_to") or {}
        pickup = do.get("pick_up_from") or {}
        containers = do.get("containers") or []

        if not containers:
            containers = [{}]

        for c in containers:
            rows.append(
                [
                    c.get("container_number", ""),          # A Container
                    normalize_size(c.get("container_type", "")),  # B Size/Type
                    shipment.get("mbl_awb", ""),            # C MBL
                    "PEGASO",                               # D CUSTOMER
                    "YATES",                                # E WAREHOUSE
                    fmt_mmddyy(shipment.get("arrival_date", "")),  # F ETA
                    "",                                     # G LFD (reserved)
                    pickup.get("company", ""),              # H TERMINAL
                    "",                                     # I Available (Y/N)
                    fmt_mmddyy(str(shipment.get("last_free_day") or "")),  # J LFD
                ]
            )
    return rows


def empty_delivery_order_payload() -> dict[str, Any]:
    return {
        "delivery_order": {
            "shipment_reference": "",
            "print_date": "",
            "issued_by": {"company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "phone": "", "email": "", "signed_by": ""},
            "shipment": {
                "carrier": "",
                "vessel_name": "",
                "voyage_flight": "",
                "port_of_origin": "",
                "mbl_awb": "",
                "hbl_ams": "",
                "it_number": "",
                "entry_number": "",
                "arrival_date": "",
                "last_free_day": None,
                "delivery_order_issued_to": "",
                "freight_terms": "",
            },
            "deliver_to": {"company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "email": "", "phone": "", "contact": ""},
            "pick_up_from": {"company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "email": "", "phone": "", "contact": ""},
            "containers": [],
        }
    }


BAD_FIELD_VALUES = {
    "ISSUED",
    "CARRIER SHOWN ABOVE",
    "SAME AS ABOVE",
    "SAME AS BILL TO",
    "N/A",
    "NA",
    "NONE",
    "UNKNOWN",
}


def clean_field(value: str) -> str:
    v = re.sub(r"\s+", " ", value or "").strip(" \t\r\n:;,-")
    v = re.split(
        r"\b(?:VESSEL|VOYAGE|ETA|ARRIVAL|MBL|HBL|DELIVER TO|PICK UP FROM|CONSIGNEE|SHIPPER|PORT OF ORIGIN)\b",
        v,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip(" \t\r\n:;,-")
    return v


def is_bad_field_value(value: str) -> bool:
    v = clean_field(value)
    if not v or len(v) < 3:
        return True
    upper = v.upper()
    if upper in BAD_FIELD_VALUES:
        return True
    if upper.startswith("CARRIER SHOWN"):
        return True
    return False


def text_lines(text: str) -> list[str]:
    out: list[str] = []
    for line in text.splitlines():
        line2 = re.sub(r"\s+", " ", line).strip()
        if line2:
            out.append(line2)
    return out


def find_labeled_value(lines: list[str], labels: list[str], value_pattern: str = r".+") -> str:
    label_re = "(?:" + "|".join(labels) + ")"
    for i, line in enumerate(lines):
        m = re.search(rf"\b{label_re}\b\s*[:#-]?\s*({value_pattern})$", line, re.IGNORECASE)
        if m:
            candidate = clean_field(m.group(1))
            if not is_bad_field_value(candidate):
                return candidate
            continue

        # Pattern where label is on one line and value is on next line.
        if re.fullmatch(rf"{label_re}\s*[:#-]?", line, re.IGNORECASE):
            if i + 1 < len(lines):
                candidate = clean_field(lines[i + 1])
                if not is_bad_field_value(candidate):
                    return candidate
    return ""


def find_deliver_to_company(lines: list[str]) -> str:
    for i, line in enumerate(lines):
        if re.search(r"\b(?:DELIVER\s*TO|CONSIGNEE)\b", line, re.IGNORECASE):
            tail = re.sub(r"^.*?(?:DELIVER\s*TO|CONSIGNEE)\s*[:#-]?\s*", "", line, flags=re.IGNORECASE).strip()
            if tail and not is_bad_field_value(tail):
                return clean_field(tail)
            for j in range(i + 1, min(i + 5, len(lines))):
                nxt = clean_field(lines[j])
                if re.search(
                    r"^(?:PICK\s*UP\s*FROM|NOTIFY|VESSEL|VOYAGE|ETA|ARRIVAL|MBL|HBL|B/L|CONTAINER|SHIPPER|PORT|TRUCKER)\b",
                    nxt,
                    re.IGNORECASE,
                ):
                    break
                if not is_bad_field_value(nxt):
                    return nxt
    return ""


def parse_weight_kg(text: str) -> float:
    m = re.search(
        r"(?:GROSS\s*WEIGHT|WEIGHT|NET\s*WEIGHT)?\s*[:#]?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:KG|KGS|KILOGRAMS?)\b",
        text,
        re.IGNORECASE,
    )
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return 0.0


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts: list[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)


def extract_container_numbers(raw_text: str, attachment_name: str = "") -> list[str]:
    from_text = set(re.findall(r"\b[A-Z]{4}\d{7}\b", raw_text))
    from_name = set(re.findall(r"\b[A-Z]{4}\d{7}\b", attachment_name.upper()))
    return sorted(from_text | from_name)


def rule_extract_delivery_order(pdf_bytes: bytes, attachment_name: str = "") -> tuple[dict[str, Any], str]:
    payload = empty_delivery_order_payload()
    text = extract_text_from_pdf(pdf_bytes)
    if not text.strip():
        return payload, text

    do = payload["delivery_order"]
    lines = text_lines(text)
    norm = " ".join(lines)

    do["shipment_reference"] = find_labeled_value(
        lines,
        ["SHIPMENT\\s*REFERENCE", "S/?REF", "REFERENCE\\s*NO\\.?", "DO\\s*NO\\.?", "DELIVERY\\s*ORDER\\s*NO\\.?"],
        r"[A-Z0-9][A-Z0-9\-_/]{4,}",
    )
    if is_bad_field_value(do["shipment_reference"]):
        do["shipment_reference"] = ""

    do["shipment"]["vessel_name"] = find_labeled_value(
        lines,
        ["VESSEL(?:\\s*NAME)?"],
        r"[A-Z0-9][A-Z0-9 ./_-]{2,80}",
    )
    do["shipment"]["arrival_date"] = find_labeled_value(
        lines,
        ["ARRIVAL\\s*DATE", "ETA"],
        r"[0-9]{1,4}[-/][0-9]{1,2}[-/][0-9]{1,4}",
    )
    do["shipment"]["mbl_awb"] = find_labeled_value(
        lines,
        ["MBL", "MAWB", "MASTER\\s*B/L", "B/L", "BOL"],
        r"[A-Z0-9][A-Z0-9\\-_/]{5,}",
    )
    do["deliver_to"]["company"] = find_deliver_to_company(lines)

    container_nos = extract_container_numbers(norm, attachment_name=attachment_name)
    total_weight = parse_weight_kg(norm)
    if container_nos:
        each_weight = round(total_weight / len(container_nos), 2) if total_weight > 0 else 0.0
        for container_no in container_nos:
            do["containers"].append(
                {
                    "container_number": container_no,
                    "container_type": "",
                    "seal_number": "",
                    "hbl_ams": "",
                    "cartons": 0,
                    "description": "",
                    "customer_ref": "",
                    "weight_lb": 0.0,
                    "weight_kg": each_weight,
                    "volume_cbm": 0.0,
                }
            )
    elif total_weight > 0:
        do["containers"].append(
            {
                "container_number": "",
                "container_type": "",
                "seal_number": "",
                "hbl_ams": "",
                "cartons": 0,
                "description": "",
                "customer_ref": "",
                "weight_lb": 0.0,
                "weight_kg": total_weight,
                "volume_cbm": 0.0,
            }
        )

    return payload, text


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api"):
        return await call_next(request)
    if path in {"/api/auth/login", "/api/auth/register"}:
        return await call_next(request)

    auth_header = request.headers.get("authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    if not token and (
        request.url.path.startswith("/api/documents/")
        or request.url.path.startswith("/api/downloads/")
    ):
        token = request.query_params.get("token", "").strip()
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


@app.post("/api/auth/register")
def register(payload: RegisterRequest) -> dict[str, Any]:
    email = payload.email.strip().lower()
    raw_password = payload.password.strip()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")
    if len(raw_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    with get_conn() as conn:
        exists = conn.execute("SELECT id FROM auth_users WHERE email = ?", (email,)).fetchone()
        if exists:
            raise HTTPException(409, "Email already registered")

        now = datetime.utcnow()
        conn.execute(
            """
            INSERT INTO auth_users(email, password_hash, role, timezone, created_at)
            VALUES (?, ?, 'customer', 'America/Los_Angeles', ?)
            """,
            (email, password_hash(raw_password), now.isoformat(timespec="seconds")),
        )
        user = conn.execute(
            "SELECT id, email, role, timezone FROM auth_users WHERE email = ?",
            (email,),
        ).fetchone()

        token = secrets.token_urlsafe(32)
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


@app.get("/api/admin/users")
def admin_list_users(request: Request) -> dict[str, Any]:
    require_role(request, {"admin"})
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, email, role, timezone, created_at
            FROM auth_users
            ORDER BY created_at DESC
            """
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.post("/api/admin/users/{user_id}/role")
def admin_update_user_role(user_id: int, payload: UserRoleUpdate, request: Request) -> dict[str, Any]:
    current = require_role(request, {"admin"})
    new_role = payload.role.strip().lower()
    if new_role not in {"customer", "operator", "admin"}:
        raise HTTPException(400, "Invalid role")

    with get_conn() as conn:
        row = conn.execute("SELECT id, role FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(404, "User not found")
        if row["id"] == current["id"] and new_role != "admin":
            raise HTTPException(400, "Admin cannot remove own admin role")

        conn.execute("UPDATE auth_users SET role = ? WHERE id = ?", (new_role, user_id))
        conn.commit()
    return {"ok": True}


@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id: int, request: Request) -> dict[str, Any]:
    current = require_role(request, {"admin"})
    if user_id == current["id"]:
        raise HTTPException(400, "Cannot delete current admin account")

    with get_conn() as conn:
        row = conn.execute("SELECT id, role FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(404, "User not found")
        if row["role"] == "admin":
            admin_count = conn.execute("SELECT COUNT(*) c FROM auth_users WHERE role = 'admin'").fetchone()["c"]
            if admin_count <= 1:
                raise HTTPException(400, "At least one admin account must remain")

        conn.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM auth_users WHERE id = ?", (user_id,))
        conn.commit()
    return {"ok": True}


@app.get("/api/overview/stats")
def overview_stats(request: Request) -> dict[str, Any]:
    user = require_user(request)
    scope_sql, scope_args = shipment_scope(user)
    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) c FROM shipments WHERE {scope_sql}", scope_args).fetchone()["c"]
        stats = {"total": total}
        for s in STATUS_ORDER:
            stats[s] = conn.execute(
                f"SELECT COUNT(*) c FROM shipments WHERE {scope_sql} AND status = ?",
                [*scope_args, s],
            ).fetchone()["c"]
        return stats


@app.get("/api/overview/shipments")
def overview_shipments(request: Request, search: str = "", status: str = "all", page: int = 1, page_size: int = 20) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        scope_sql, scope_args = shipment_scope(user)
        where = [scope_sql]
        args: list[Any] = [*scope_args]
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
    request: Request,
    search: str = "",
    status: str = "all",
    today_pickup: bool = False,
    next_day_pickup: bool = False,
    pre_pull_only: bool = False,
    sort: str = "created_desc",
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    user = require_user(request)
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

    scope_sql, scope_args = shipment_scope(user)
    where = [scope_sql]
    args: list[Any] = [*scope_args]

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
def create_shipment(payload: ShipmentCreate, request: Request) -> dict[str, Any]:
    user = require_user(request)
    if payload.status not in STATUS_ORDER:
        raise HTTPException(400, "Invalid status")

    def val(v: str) -> str | None:
        s = v.strip()
        return s if s else None

    owner_user_id = user["id"] if user["role"] == "customer" else None
    now = datetime.utcnow().isoformat(timespec="seconds")
    with get_conn() as conn:
        try:
            conn.execute(
                """
                INSERT INTO shipments(
                  shipment_id, owner_user_id, container_no, mbol, size, terminal, carrier, eta_at, lfd_at, dg,
                  deliver_company, deliver_to, warehouse_contact, warehouse_phone, remark,
                  pickup_appt_at, scheduled_delivery_at, actual_delivery_at, empty_date_at, empty_return_at,
                  waiting_port_minutes, waiting_local_minutes, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, 0, ?, ?, ?)
                """,
                (
                    payload.shipment_id.strip(),
                    owner_user_id,
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
def shipment_detail(shipment_id: str, request: Request) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        if user["role"] == "customer" and row["owner_user_id"] != user["id"]:
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
def delete_shipment(shipment_id: str, request: Request) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        row = conn.execute("SELECT id, owner_user_id FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        if user["role"] == "customer" and row["owner_user_id"] != user["id"]:
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
        row = conn.execute("SELECT id, owner_user_id FROM shipments WHERE shipment_id = ?", (shipment_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Shipment not found")
        if user["role"] == "customer" and row["owner_user_id"] != user["id"]:
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
def download_document(doc_id: int, request: Request) -> FileResponse:
    user = require_user(request)
    with get_conn() as conn:
        doc = conn.execute(
            """
            SELECT d.*, s.owner_user_id
            FROM shipment_documents d
            JOIN shipments s ON s.id = d.shipment_id
            WHERE d.id = ?
            """,
            (doc_id,),
        ).fetchone()
        if not doc:
            raise HTTPException(404, "Document not found")
        if user["role"] == "customer" and doc["owner_user_id"] != user["id"]:
            raise HTTPException(404, "Document not found")
        file_path = Path(doc["file_path"])
        if not file_path.exists():
            raise HTTPException(404, "File missing")

        conn.execute("UPDATE shipment_documents SET downloaded = 1 WHERE id = ?", (doc_id,))
        conn.commit()

        return FileResponse(path=file_path, filename=doc["file_name"], media_type="application/octet-stream")


@app.get("/api/empty-returns")
def empty_returns(request: Request, search: str = "", page: int = 1, page_size: int = 20) -> dict[str, Any]:
    user = require_user(request)
    statuses = ("delivered", "empty_date_confirmed", "empty_returned")
    scope_sql, scope_args = shipment_scope(user)
    where = [scope_sql, "status IN (?, ?, ?)"]
    args: list[Any] = [*scope_args, *list(statuses)]
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
def do_download_list(request: Request, search: str = "", today_pickup: bool = False, next_day_pickup: bool = False) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        where = ["1=1"]
        args: list[Any] = []
        if user["role"] == "customer":
            where.append("s.owner_user_id = ?")
            args.append(user["id"])
        if search:
            like = f"%{search}%"
            where.append("(s.shipment_id LIKE ? OR s.container_no LIKE ? OR s.deliver_to LIKE ? OR d.file_name LIKE ?)")
            args.extend([like, like, like, like])
        now = datetime.now()
        if today_pickup:
            where.append("date(s.pickup_appt_at) = date(?)")
            args.append(now.isoformat(sep=" "))
        if next_day_pickup:
            where.append("date(s.pickup_appt_at) = date(?, '+1 day')")
            args.append(now.isoformat(sep=" "))
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
def do_batch_download(shipment_ids: list[str], request: Request) -> dict[str, Any]:
    user = require_user(request)
    if not shipment_ids:
        raise HTTPException(400, "No shipments selected")

    with get_conn() as conn:
        placeholders = ",".join("?" for _ in shipment_ids)
        where_scope = ""
        args: list[Any] = list(shipment_ids)
        if user["role"] == "customer":
            where_scope = " AND s.owner_user_id = ?"
            args.append(user["id"])
        rows = conn.execute(
            f"""
            SELECT d.id as doc_id, d.file_path, d.file_name, s.shipment_id
            FROM shipment_documents d
            JOIN shipments s ON s.id = d.shipment_id
            WHERE s.shipment_id IN ({placeholders}) AND d.doc_type = 'DO' AND d.is_latest = 1 {where_scope}
            """,
            args,
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
def pod_upload_list(request: Request, search: str = "") -> dict[str, Any]:
    user = require_user(request)
    where = ["s.status = 'dispatched'"]
    args: list[Any] = []
    if user["role"] == "customer":
        where.append("s.owner_user_id = ?")
        args.append(user["id"])
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
def list_tickets(request: Request) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        sql = """
            SELECT t.ticket_no, s.shipment_id, t.category, t.attachment_name, t.status, t.created_at
            FROM tickets t
            LEFT JOIN shipments s ON s.id = t.shipment_id
            """
        args: list[Any] = []
        if user["role"] == "customer":
            sql += " WHERE s.owner_user_id = ?"
            args.append(user["id"])
        sql += " ORDER BY t.created_at DESC"
        rows = conn.execute(sql, args).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.post("/api/tickets")
def create_ticket(payload: TicketCreate, request: Request) -> dict[str, Any]:
    user = require_user(request)
    with get_conn() as conn:
        ticket_no = f"T{int(datetime.utcnow().timestamp())}"
        shipment_pk = None
        if payload.shipment_id:
            row = conn.execute("SELECT id, owner_user_id FROM shipments WHERE shipment_id = ?", (payload.shipment_id,)).fetchone()
            if row:
                if user["role"] == "customer" and row["owner_user_id"] != user["id"]:
                    raise HTTPException(404, "Shipment not found")
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


@app.get("/gmail-do")
def gmail_do_page() -> FileResponse:
    page_path = FRONTEND_DIR / "gmail_do.html"
    if not page_path.exists():
        raise HTTPException(404, "gmail_do.html not found")
    return FileResponse(page_path)


@app.get("/gmail/session")
def gmail_session(request: Request) -> JSONResponse:
    sid, session, created = get_or_create_gmail_session(request)
    profile = session.get("gmail_profile", {})
    mode = session.get("auth_mode") or ""
    authenticated = bool(session.get("access_token") or session.get("imap_config"))
    email_addr = profile.get("emailAddress")
    if mode == "imap":
        email_addr = (session.get("imap_config") or {}).get("email", "")
    resp = JSONResponse(
        {
            "authenticated": authenticated,
            "email": email_addr,
            "mode": mode or None,
        }
    )
    if created:
        set_gmail_cookie(resp, sid)
    return resp


@app.post("/imap/connect")
def imap_connect(payload: ImapConnectRequest, request: Request) -> JSONResponse:
    sid, session, created = get_or_create_gmail_session(request)
    email_addr = payload.email.strip().lower()
    host = payload.host.strip()
    port = int(payload.port or 993)
    if not email_addr or not host:
        raise HTTPException(400, "IMAP email and host are required.")

    cfg = {
        "email": email_addr,
        "password": payload.password,
        "host": host,
        "port": port,
    }
    client = imap_open(cfg)
    client.logout()

    session["created_at_ts"] = session.get("created_at_ts") or datetime.utcnow().timestamp()
    session["auth_mode"] = "imap"
    session["imap_config"] = cfg

    resp = JSONResponse({"ok": True, "mode": "imap", "email": email_addr})
    if created:
        set_gmail_cookie(resp, sid)
    return resp


@app.get("/gmail/auth/start")
def gmail_auth_start(request: Request) -> JSONResponse:
    sid, session, created = get_or_create_gmail_session(request)
    client_id = gmail_env("GOOGLE_CLIENT_ID", required=True)
    redirect_uri = gmail_env("GOOGLE_OAUTH_REDIRECT_URI", str(request.base_url) + "gmail/auth/callback")

    state = secrets.token_urlsafe(24)
    session["oauth_state"] = state
    session["oauth_redirect_uri"] = redirect_uri
    if "auth_mode" not in session:
        session["auth_mode"] = "gmail_oauth"

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/spreadsheets",
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    resp = JSONResponse({"auth_url": auth_url})
    if created:
        set_gmail_cookie(resp, sid)
    return resp


@app.get("/gmail/auth/callback")
def gmail_auth_callback(request: Request, code: str = "", state: str = "", error: str = "") -> RedirectResponse:
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    session = GMAIL_OAUTH_SESSIONS.get(sid)
    if not session:
        raise HTTPException(400, "OAuth session missing. Please retry sign-in.")
    if error:
        raise HTTPException(400, f"Google OAuth error: {error}")
    if not code:
        raise HTTPException(400, "Missing Google OAuth code.")
    if state != session.get("oauth_state"):
        raise HTTPException(400, "OAuth state mismatch.")

    redirect_uri = session.get("oauth_redirect_uri") or gmail_env("GOOGLE_OAUTH_REDIRECT_URI", str(request.base_url) + "gmail/auth/callback")
    token_data = google_token_request(
        {
            "client_id": gmail_env("GOOGLE_CLIENT_ID", required=True),
            "client_secret": gmail_env("GOOGLE_CLIENT_SECRET", required=True),
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
    )

    access_token = token_data.get("access_token", "")
    if not access_token:
        raise HTTPException(400, "Google token response missing access_token.")

    session["access_token"] = access_token
    refresh_token = token_data.get("refresh_token")
    if refresh_token:
        session["refresh_token"] = refresh_token
    session["expires_at_ts"] = datetime.utcnow().timestamp() + int(token_data.get("expires_in", 3600)) - 30
    session["created_at_ts"] = datetime.utcnow().timestamp()
    session["gmail_profile"] = get_gmail_profile(session)
    if not session.get("auth_mode"):
        session["auth_mode"] = "gmail_oauth"
    session.pop("oauth_state", None)
    session.pop("oauth_redirect_uri", None)

    resp = RedirectResponse(url="/gmail-do?connected=1", status_code=302)
    set_gmail_cookie(resp, sid)
    return resp


@app.post("/gmail/logout")
def gmail_logout(request: Request) -> JSONResponse:
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    if sid:
        GMAIL_OAUTH_SESSIONS.pop(sid, None)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(GMAIL_COOKIE_NAME)
    return resp


@app.get("/gmail/messages")
def gmail_messages(
    request: Request,
    q: str = "in:anywhere has:attachment (subject:(DO OR \"delivery order\" OR 提柜预报) OR filename:(DO OR Delivery_Order)) newer_than:90d",
    max_results: int = 15,
    incoming_only: bool = True,
) -> JSONResponse:
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    session = GMAIL_OAUTH_SESSIONS.get(sid)
    if not session:
        raise HTTPException(401, "Not connected.")

    mode = session.get("auth_mode") or "gmail_oauth"
    max_results = max(1, min(max_results, 200))

    if mode == "imap":
        cfg = imap_config_from_session(session)
        newer_days = parse_query_newer_than_days(q, default_days=14)
        subject_terms, file_terms = extract_subject_filename_terms(q)
        if not subject_terms and not file_terms:
            subject_terms = ["do", "delivery order", "提柜预报"]
            file_terms = ["do", "delivery_order"]

        cutoff = datetime.utcnow() - timedelta(days=newer_days)
        client = imap_open(cfg)
        items: list[dict[str, Any]] = []
        try:
            client.select("INBOX", readonly=True)
            typ, data = client.uid("search", None, "ALL")
            if typ != "OK":
                raise HTTPException(400, "IMAP search failed.")
            uid_bytes = (data[0] or b"").split()
            for uid_b in reversed(uid_bytes):
                uid = uid_b.decode("utf-8", errors="ignore")
                raw_bytes = imap_fetch_message_bytes(client, uid)
                msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)

                msg_dt = header_to_datetime(msg)
                if msg_dt and msg_dt < cutoff:
                    continue

                from_addr = str(msg.get("From", ""))
                from_email = extract_email_address(from_addr)
                is_outgoing = bool(from_email and from_email == cfg["email"].lower())
                if incoming_only and is_outgoing:
                    continue

                subject = str(msg.get("Subject", "(no subject)"))
                pdf_parts = collect_imap_pdf_parts(msg)
                if not pdf_parts:
                    continue

                subj_lower = subject.lower()
                file_lower = " ".join([(p.get("filename") or "").lower() for p in pdf_parts])
                matched_subject = (not subject_terms) or any(t in subj_lower for t in subject_terms)
                matched_file = (not file_terms) or any(t in file_lower for t in file_terms)
                if not (matched_subject or matched_file):
                    continue

                items.append(
                    {
                        "id": uid,
                        "thread_id": "",
                        "from": from_addr,
                        "subject": subject,
                        "date": str(msg.get("Date", "")),
                        "snippet": "",
                        "pdf_count": len(pdf_parts),
                        "pdf_files": [p["filename"] for p in pdf_parts],
                        "is_outgoing": is_outgoing,
                    }
                )
                if len(items) >= max_results:
                    break
        finally:
            try:
                client.logout()
            except Exception:
                pass

        return JSONResponse({"items": items})

    if not session.get("access_token"):
        raise HTTPException(401, "Not connected to Gmail.")

    profile = session.get("gmail_profile") or get_gmail_profile(session)
    me_email = extract_email_address((profile or {}).get("emailAddress", ""))
    query = urllib.parse.urlencode({"q": q, "maxResults": str(max_results)})
    list_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?{query}"
    listed = gmail_api_request(session, list_url)
    items: list[dict[str, Any]] = []
    for msg in listed.get("messages") or []:
        msg_id = msg.get("id")
        if not msg_id:
            continue
        detail_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}?format=full"
        detail = gmail_api_request(session, detail_url)
        payload = detail.get("payload") or {}
        headers = parse_header_map(payload.get("headers"))
        from_addr = headers.get("from", "")
        from_email = extract_email_address(from_addr)
        is_outgoing = bool(me_email and from_email and from_email == me_email)
        if incoming_only and is_outgoing:
            continue
        pdf_parts: list[dict[str, Any]] = []
        collect_pdf_parts(payload, pdf_parts)
        items.append(
            {
                "id": msg_id,
                "thread_id": detail.get("threadId"),
                "from": from_addr,
                "subject": headers.get("subject", "(no subject)"),
                "date": headers.get("date", ""),
                "snippet": detail.get("snippet", ""),
                "pdf_count": len(pdf_parts),
                "pdf_files": [p["filename"] for p in pdf_parts],
                "is_outgoing": is_outgoing,
            }
        )
    return JSONResponse({"items": items})


@app.post("/gmail/process")
def gmail_process(payload: GmailProcessRequest, request: Request) -> dict[str, Any]:
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    session = GMAIL_OAUTH_SESSIONS.get(sid)
    if not session:
        raise HTTPException(401, "Not connected.")

    anthropic_key = gmail_env("ANTHROPIC_API_KEY", required=True)
    anthropic_model = gmail_env("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    anthropic_max_tokens = int(gmail_env("ANTHROPIC_MAX_TOKENS", "2048"))

    mode = session.get("auth_mode") or "gmail_oauth"
    headers: dict[str, str] = {}
    pdf_parts: list[dict[str, Any]] = []
    message_meta: dict[str, Any] = {"id": payload.message_id, "subject": "(no subject)", "from": "", "date": ""}

    if mode == "imap":
        cfg = imap_config_from_session(session)
        client = imap_open(cfg)
        try:
            client.select("INBOX", readonly=True)
            raw_bytes = imap_fetch_message_bytes(client, payload.message_id)
            msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)
            message_meta["subject"] = str(msg.get("Subject", "(no subject)"))
            message_meta["from"] = str(msg.get("From", ""))
            message_meta["date"] = str(msg.get("Date", ""))
            for part in collect_imap_pdf_parts(msg):
                pdf_parts.append(
                    {
                        "filename": part["filename"],
                        "pdf_bytes": part["bytes"],
                    }
                )
        finally:
            try:
                client.logout()
            except Exception:
                pass
    else:
        if not session.get("access_token"):
            raise HTTPException(401, "Not connected to Gmail.")
        detail_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{payload.message_id}?format=full"
        detail = gmail_api_request(session, detail_url)
        message_payload = detail.get("payload") or {}
        headers = parse_header_map(message_payload.get("headers"))
        message_meta["subject"] = headers.get("subject", "(no subject)")
        message_meta["from"] = headers.get("from", "")
        message_meta["date"] = headers.get("date", "")
        collect_pdf_parts(message_payload, pdf_parts)

    if not pdf_parts:
        raise HTTPException(400, "No PDF attachments found in this message.")

    extracted_items: list[dict[str, Any]] = []
    for part in pdf_parts:
        filename = part["filename"]
        attachment_id = part.get("attachment_id")
        inline_data = part.get("inline_data")
        pdf_bytes: bytes | None = None

        if part.get("pdf_bytes") is not None:
            pdf_bytes = part.get("pdf_bytes")
        elif inline_data:
            pdf_bytes = decode_base64url(inline_data)
        elif attachment_id:
            attachment_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{payload.message_id}/attachments/{attachment_id}"
            attachment = gmail_api_request(session, attachment_url)
            data = attachment.get("data")
            if data:
                pdf_bytes = decode_base64url(data)

        if not pdf_bytes:
            extracted_items.append({"filename": filename, "error": "Unable to read attachment content."})
            continue

        try:
            extracted = anthropic_extract(
                pdf_bytes=pdf_bytes,
                api_key=anthropic_key,
                model=anthropic_model,
                max_tokens=anthropic_max_tokens,
            )
            do_data = (extracted or {}).get("delivery_order", {})
            total_kg = 0.0
            for container in do_data.get("containers") or []:
                try:
                    total_kg += float(container.get("weight_kg") or 0)
                except (TypeError, ValueError):
                    continue
            extracted_items.append(
                {
                    "filename": filename,
                    "shipment_reference": do_data.get("shipment_reference"),
                    "container_count": len(do_data.get("containers") or []),
                    "total_weight_kg": round(total_kg, 2),
                    "result": extracted,
                }
            )
        except Exception as exc:
            extracted_items.append({"filename": filename, "error": str(exc)})

    return {
        "message": message_meta,
        "attachments": extracted_items,
    }


@app.post("/gmail/process-free")
def gmail_process_free(payload: GmailProcessRequest, request: Request) -> dict[str, Any]:
    sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
    session = GMAIL_OAUTH_SESSIONS.get(sid)
    if not session:
        raise HTTPException(401, "Not connected.")

    mode = session.get("auth_mode") or "gmail_oauth"
    headers: dict[str, str] = {}
    pdf_parts: list[dict[str, Any]] = []
    message_meta: dict[str, Any] = {"id": payload.message_id, "subject": "(no subject)", "from": "", "date": ""}

    if mode == "imap":
        cfg = imap_config_from_session(session)
        client = imap_open(cfg)
        try:
            client.select("INBOX", readonly=True)
            raw_bytes = imap_fetch_message_bytes(client, payload.message_id)
            msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)
            message_meta["subject"] = str(msg.get("Subject", "(no subject)"))
            message_meta["from"] = str(msg.get("From", ""))
            message_meta["date"] = str(msg.get("Date", ""))
            for part in collect_imap_pdf_parts(msg):
                pdf_parts.append(
                    {
                        "filename": part["filename"],
                        "pdf_bytes": part["bytes"],
                    }
                )
        finally:
            try:
                client.logout()
            except Exception:
                pass
    else:
        if not session.get("access_token"):
            raise HTTPException(401, "Not connected to Gmail.")
        detail_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{payload.message_id}?format=full"
        detail = gmail_api_request(session, detail_url)
        message_payload = detail.get("payload") or {}
        headers = parse_header_map(message_payload.get("headers"))
        message_meta["subject"] = headers.get("subject", "(no subject)")
        message_meta["from"] = headers.get("from", "")
        message_meta["date"] = headers.get("date", "")
        collect_pdf_parts(message_payload, pdf_parts)

    if not pdf_parts:
        raise HTTPException(400, "No PDF attachments found in this message.")

    extracted_items: list[dict[str, Any]] = []
    for part in pdf_parts:
        filename = part["filename"]
        attachment_id = part.get("attachment_id")
        inline_data = part.get("inline_data")
        pdf_bytes: bytes | None = None

        if part.get("pdf_bytes") is not None:
            pdf_bytes = part.get("pdf_bytes")
        elif inline_data:
            pdf_bytes = decode_base64url(inline_data)
        elif attachment_id:
            attachment_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{payload.message_id}/attachments/{attachment_id}"
            attachment = gmail_api_request(session, attachment_url)
            data = attachment.get("data")
            if data:
                pdf_bytes = decode_base64url(data)

        if not pdf_bytes:
            extracted_items.append({"filename": filename, "error": "Unable to read attachment content."})
            continue

        try:
            extracted, raw_text = rule_extract_delivery_order(pdf_bytes, attachment_name=filename)
            do_data = (extracted or {}).get("delivery_order", {})
            total_kg = 0.0
            for container in do_data.get("containers") or []:
                try:
                    total_kg += float(container.get("weight_kg") or 0)
                except (TypeError, ValueError):
                    continue
            extracted_items.append(
                {
                    "filename": filename,
                    "shipment_reference": do_data.get("shipment_reference"),
                    "container_count": len(do_data.get("containers") or []),
                    "total_weight_kg": round(total_kg, 2),
                    "debug_text": (raw_text or "")[:12000],
                    "result": extracted,
                }
            )
        except Exception as exc:
            extracted_items.append({"filename": filename, "error": str(exc)})

    return {
        "message": message_meta,
        "attachments": extracted_items,
    }


@app.post("/sheets/append")
def sheets_append(payload: SheetAppendRequest, request: Request) -> dict[str, Any]:
    try:
        sid = request.cookies.get(GMAIL_COOKIE_NAME, "")
        session = GMAIL_OAUTH_SESSIONS.get(sid)
        if not session:
            raise HTTPException(401, "Not connected to Google.")
        auth_mode = session.get("auth_mode") or "gmail_oauth"
        has_oauth = bool(session.get("access_token"))

        sheet_id = extract_sheet_id(payload.sheet_id_or_url)
        sheet_gid = extract_sheet_gid(payload.sheet_id_or_url)
        rows = to_sheet_rows(payload.attachments)
        if not rows:
            raise HTTPException(400, "No extracted attachment results to append.")

        if has_oauth:
            sheet_title = resolve_sheet_title(
                sheet_id=sheet_id,
                preferred_name=payload.sheet_name,
                gid=sheet_gid,
                session=session,
            )
            next_row = get_next_sheet_row_index(
                sheet_id=sheet_id,
                sheet_title=sheet_title,
                session=session,
            )
        else:
            sa_token = get_service_account_sheets_token()
            sheet_title = resolve_sheet_title(
                sheet_id=sheet_id,
                preferred_name=payload.sheet_name,
                gid=sheet_gid,
                bearer_token=sa_token,
            )
            next_row = get_next_sheet_row_index(
                sheet_id=sheet_id,
                sheet_title=sheet_title,
                bearer_token=sa_token,
            )

        end_row = next_row + len(rows) - 1
        target_range = f"{quote_sheet_title(sheet_title)}!A{next_row}:J{end_row}"
        encoded_range = urllib.parse.quote(target_range, safe="!:")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded_range}?valueInputOption=USER_ENTERED"
        if has_oauth:
            resp = gmail_api_request(session, url, method="PUT", body={"values": rows})
        else:
            resp = google_api_request_with_bearer(sa_token, url, method="PUT", body={"values": rows})
    except HTTPException as exc:
        msg = str(exc.detail)
        if "insufficientPermissions" in msg or "ACCESS_TOKEN_SCOPE_INSUFFICIENT" in msg:
            raise HTTPException(
                403,
                "Google Sheets permission missing. Please click Disconnect, then Sign in with Google again to grant Sheets access.",
            ) from exc
        if (
            auth_mode == "imap"
            and "IMAP mode push requires GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE" in msg
        ):
            raise HTTPException(
                400,
                "To push in IMAP mode, connect Google once (provider: Google OAuth) in the same browser session, or configure GOOGLE_SERVICE_ACCOUNT_FILE.",
            ) from exc
        raise
    except Exception as exc:
        raise HTTPException(400, f"Sheets append failed: {exc}") from exc

    updates = (resp or {})
    return {
        "ok": True,
        "sheet_id": sheet_id,
        "sheet_name": sheet_title,
        "rows_appended": updates.get("updatedRows", len(rows)),
        "range": updates.get("updatedRange", target_range),
    }


app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

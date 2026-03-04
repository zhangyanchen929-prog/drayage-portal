import argparse
import base64
import datetime as dt
import email
import imaplib
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any


SYSTEM_PROMPT = """You are a logistics document parser. Extract all data from the delivery order PDF and return ONLY a valid JSON object - no markdown, no explanation, no extra text.

Use this exact structure:
{
  "delivery_order": {
    "shipment_reference": "",
    "print_date": "YYYY-MM-DD",
    "issued_by": { "company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "phone": "", "email": "", "signed_by": "" },
    "shipment": { "carrier": "", "vessel_name": "", "voyage_flight": "", "port_of_origin": "", "mbl_awb": "", "hbl_ams": "", "it_number": "", "entry_number": "", "arrival_date": "YYYY-MM-DD", "last_free_day": null, "delivery_order_issued_to": "", "freight_terms": "" },
    "deliver_to": { "company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "email": "", "phone": "", "contact": "" },
    "pick_up_from": { "company": "", "address": "", "city": "", "state": "", "zip": "", "country": "", "email": "", "phone": "", "contact": "" },
    "containers": [ { "container_number": "", "container_type": "", "seal_number": "", "hbl_ams": "", "cartons": 0, "description": "", "customer_ref": "", "weight_lb": 0.0, "weight_kg": 0.0, "volume_cbm": 0.0 } ]
  }
}"""


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return value or ""


def to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name)


def load_processed_uids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data if isinstance(data, list) else [])
    except Exception:
        return set()


def save_processed_uids(path: Path, uids: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(uids), indent=2), encoding="utf-8")


def extract_pdf_attachments(message: email.message.EmailMessage) -> list[tuple[str, bytes]]:
    found: list[tuple[str, bytes]] = []
    for part in message.iter_attachments():
        filename = part.get_filename() or "attachment.pdf"
        content_type = (part.get_content_type() or "").lower()
        payload = part.get_payload(decode=True)

        if not payload:
            continue
        if content_type == "application/pdf" or filename.lower().endswith(".pdf"):
            found.append((sanitize_filename(filename), payload))

    return found


def anthropic_extract(pdf_bytes: bytes, api_key: str, model: str, max_tokens: int) -> dict[str, Any]:
    b64_pdf = base64.b64encode(pdf_bytes).decode("ascii")

    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": b64_pdf,
                        },
                    },
                    {
                        "type": "text",
                        "text": "Extract all data from this delivery order and return as JSON.",
                    },
                ],
            }
        ],
    }

    req = urllib.request.Request(
        url="https://api.anthropic.com/v1/messages",
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Anthropic API HTTP {exc.code}: {detail}") from exc

    data = json.loads(raw)
    blocks = data.get("content") or []
    text_chunks = [b.get("text", "") for b in blocks if b.get("type") == "text"]
    text = "\n".join(text_chunks).strip()

    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"```$", "", text).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        preview = text[:500].replace("\n", " ")
        raise RuntimeError(f"Model returned non-JSON response: {preview}") from exc


def connect_imap(host: str, port: int, username: str, password: str) -> imaplib.IMAP4_SSL:
    ssl_context = ssl.create_default_context()
    client = imaplib.IMAP4_SSL(host=host, port=port, ssl_context=ssl_context)
    client.login(username, password)
    return client


def process_once() -> int:
    imap_host = env("IMAP_HOST", required=True)
    imap_port = int(env("IMAP_PORT", "993"))
    imap_user = env("IMAP_USER", required=True)
    imap_pass = env("IMAP_PASS", required=True)
    imap_folder = env("IMAP_FOLDER", "INBOX")
    imap_search = env("IMAP_SEARCH", "UNSEEN")

    anthropic_key = env("ANTHROPIC_API_KEY", required=True)
    anthropic_model = env("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    anthropic_max_tokens = int(env("ANTHROPIC_MAX_TOKENS", "2048"))

    output_dir = Path(env("EMAIL_EXTRACT_OUTPUT_DIR", "data/email_extractions"))
    state_file = Path(env("EMAIL_EXTRACT_STATE_FILE", "data/email_extractions/processed_uids.json"))
    mark_seen = to_bool(env("EMAIL_EXTRACT_MARK_SEEN", "true"), default=True)

    processed = load_processed_uids(state_file)
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_in_this_run = 0

    with connect_imap(imap_host, imap_port, imap_user, imap_pass) as client:
        client.select(imap_folder)
        status, data = client.uid("search", None, imap_search)
        if status != "OK":
            raise RuntimeError(f"IMAP search failed: {data}")

        uids = data[0].split() if data and data[0] else []
        print(f"Found {len(uids)} matching emails in {imap_folder}.")

        for uid_bytes in uids:
            uid = uid_bytes.decode("utf-8")
            if uid in processed:
                continue

            status, msg_data = client.uid("fetch", uid, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                print(f"Skip UID {uid}: unable to fetch message")
                continue

            raw_msg = msg_data[0][1]
            message = BytesParser(policy=policy.default).parsebytes(raw_msg)
            subject = message.get("subject", "(no subject)")
            from_addr = message.get("from", "(unknown)")

            pdfs = extract_pdf_attachments(message)
            if not pdfs:
                print(f"UID {uid}: no PDF attachment, skipping")
                processed.add(uid)
                continue

            print(f"UID {uid}: processing {len(pdfs)} PDF(s) | subject={subject}")
            all_ok = True

            for index, (filename, pdf_bytes) in enumerate(pdfs, start=1):
                try:
                    extracted = anthropic_extract(
                        pdf_bytes=pdf_bytes,
                        api_key=anthropic_key,
                        model=anthropic_model,
                        max_tokens=anthropic_max_tokens,
                    )

                    delivery = extracted.get("delivery_order", {})
                    ref = sanitize_filename((delivery.get("shipment_reference") or "unknown_ref")[:64])
                    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                    out_name = f"{stamp}_uid{uid}_{index}_{ref}.json"
                    out_path = output_dir / out_name

                    payload = {
                        "meta": {
                            "uid": uid,
                            "from": from_addr,
                            "subject": subject,
                            "attachment": filename,
                            "processed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        },
                        "result": extracted,
                    }
                    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                    print(f"Saved: {out_path}")

                except Exception as exc:
                    all_ok = False
                    print(f"UID {uid} | attachment {filename} failed: {exc}", file=sys.stderr)

            if all_ok:
                processed.add(uid)
                processed_in_this_run += 1
                if mark_seen:
                    client.uid("store", uid, "+FLAGS", "(\\Seen)")
            else:
                print(f"UID {uid}: kept unprocessed for retry")

    save_processed_uids(state_file, processed)
    print(f"Done. Successfully processed {processed_in_this_run} email(s).")
    return processed_in_this_run


def run() -> None:
    parser = argparse.ArgumentParser(
        description="Auto-extract delivery order data from email PDF attachments"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one polling cycle then exit",
    )
    args = parser.parse_args()

    poll_seconds = int(env("EMAIL_EXTRACT_POLL_SECONDS", "60"))

    if args.once:
        process_once()
        return

    while True:
        try:
            process_once()
        except Exception as exc:
            print(f"Cycle failed: {exc}", file=sys.stderr)

        print(f"Sleep {poll_seconds}s before next cycle...")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    run()

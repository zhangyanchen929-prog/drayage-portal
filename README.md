# Drayage Portal MVP

A runnable MVP for a 99%-similar drayage operations portal based on your observed modules.

## Included modules
- Overview
- Shipment + Detail Drawer
- Empty Return
- Download DO (single + batch)
- Upload POD
- Tickets
- Pricing

## Tech
- FastAPI + SQLite
- Vanilla JS frontend

## Run
```bash
cd /Users/yanchenzhang/Documents/New\ project
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```
Open: [http://localhost:8000](http://localhost:8000)

## Data paths
- DB: `/Users/yanchenzhang/Documents/New project/data/drayage.db`
- Uploads: `/Users/yanchenzhang/Documents/New project/uploads`
- Batch ZIP: `/Users/yanchenzhang/Documents/New project/downloads`

## Blank Page Troubleshooting
If page is blank, it is usually because backend is not running or you opened local file directly.
1. Start backend with `python run.py`
2. Open exactly [http://localhost:8000](http://localhost:8000)
3. Check API quick test: [http://localhost:8000/api/overview/stats](http://localhost:8000/api/overview/stats)

## Notes
- Current auth supports login + roles (`customer` / `operator`).
- Production建议：S3文件存储、Redis任务队列、JWT+RBAC。

## Email Auto Extraction (Delivery Order PDF -> JSON)
The project now includes an email polling worker:
- Reads emails from IMAP (`UNSEEN` by default)
- Finds PDF attachments
- Sends each PDF to Anthropic for structured extraction
- Saves JSON output to `data/email_extractions`

### Setup
1. Copy `.env.email.example` and fill in values:
   - IMAP mailbox credentials
   - `ANTHROPIC_API_KEY`
2. Export environment variables in your shell.

### Run Once (test)
```bash
cd /Users/yanchenzhang/Documents/New\ project
source .venv/bin/activate
set -a; source .env.email.example; set +a
python -m backend.email_extractor --once
```

### Run Continuously
```bash
cd /Users/yanchenzhang/Documents/New\ project
source .venv/bin/activate
set -a; source .env.email.example; set +a
python -m backend.email_extractor
```

Output:
- Extracted files: `data/email_extractions/*.json`
- Processed UID state: `data/email_extractions/processed_uids.json`

## Gmail OAuth Web Extractor (Shareable URL)
This repo also includes a web page at `/gmail-do`:
- User clicks `Sign in with Google`
- App reads their Gmail (readonly)
- Finds DO emails with PDF attachments
- Free mode: parses PDF text with local rules (no Anthropic cost)
- Optional paid mode endpoint remains available if Anthropic key is configured

### Required env vars
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_OAUTH_REDIRECT_URI` (must match Google Console config)
- `ANTHROPIC_API_KEY` (only needed for paid `/gmail/process` endpoint)

### Google setup checklist
1. Create OAuth client in Google Cloud Console (Web application).
2. Add redirect URI:
   - local: `http://localhost:8000/gmail/auth/callback`
   - prod: `https://<your-domain>/gmail/auth/callback`
3. Enable Gmail API in the same Google project.

### Open
- `http://localhost:8000/gmail-do`

## Deploy (Render / Railway)
This repo is now deployment-ready:
- `render.yaml` (Render one-click setup)
- `railway.json` (Railway setup)
- `run.py` reads `PORT` from environment

### Quick steps
1. Push project to GitHub.
2. Create a new Web Service on Render or a new Project on Railway.
3. Connect the GitHub repo.
4. Start command: `python run.py`
5. Build command: `pip install -r requirements.txt`
6. After deploy, open the generated URL and log in with:
   - `customer@demo.com / customer123`
   - `operator@demo.com / operator123`

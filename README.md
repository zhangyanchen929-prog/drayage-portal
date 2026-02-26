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

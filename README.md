# GKM — Tax Returns Contracted vs Received

This folder contains a full-stack MVP:

- **Backend**: FastAPI + Postgres (JWT auth, Excel ingestion, dashboard APIs)
- **Frontend**: React (Vite) (Login, Documents Processing upload, Dashboard)

## Prerequisites

- Node.js 18+ (or 20+)
- Python 3.12+
- Docker Desktop (recommended) or any Postgres 16 instance

## 1) Start Postgres

If you use Docker Desktop:

```bash
cd "/Users/yavar/Documents/CoE/GKM"
docker compose up -d
```

## 2) Backend (FastAPI)

```bash
cd "/Users/yavar/Documents/CoE/GKM/backend"
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp -n .env.example .env
# edit JWT_SECRET if desired

alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

Default seeded admin (from `backend/.env`):
- email: `admin@example.com`
- password: `admin1234`

## 3) Frontend (React)

```bash
cd "/Users/yavar/Documents/CoE/GKM/frontend"
npm install
npm run dev
```

Open `http://localhost:5173`.

## 4) Load sample data

Go to **Documents Processing** and upload:
- `data/Contracted vs Actual -2026.xlsx`

Then open **Dashboard**.


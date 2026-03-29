# GKM — Tax Returns Contracted vs Received

Full-stack application for managing tax return data and generating dashboards from Excel workbooks.

## Stack

- **Backend**: FastAPI + PostgreSQL (JWT auth, Excel ingestion, dashboard APIs, PII handling)
- **Frontend**: React 19 + TypeScript + Vite (Login, document processing, interactive dashboards)
- **Database**: PostgreSQL 16 with Docker Compose

## Features

- Excel workbook upload and processing with PII detection/redaction
- Dashboard blueprint generation with AI-assisted proposals
- Interactive dashboards with KPIs, client tables, and staff lists
- PDF report generation (overall and client-specific)
- Role-based access control (admin, analyst, client_viewer)

## Quick Start

### Prerequisites

- Node.js 18+ (or 20+)
- Python 3.12+
- Docker Desktop (recommended) or PostgreSQL 16

### 1. Start PostgreSQL

```bash
cd "/Users/yavar/Documents/CoE/GKM"
docker compose up -d
```

### 2. Backend Setup

```bash
cd "/Users/yavar/Documents/CoE/GKM/backend"
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp -n .env.example .env
# Edit JWT_SECRET in .env if desired

alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

Default admin credentials:
- Email: `admin@example.com`
- Password: `admin1234`

### 3. Frontend Setup

```bash
cd "/Users/yavar/Documents/CoE/GKM/frontend"
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173).

### 4. Load Sample Data

1. Login with admin credentials
2. Go to **Documents** page
3. Upload `data/Contracted vs Actual -2026.xlsx`
4. Open **Dashboard** to view the data

## Documentation

See [CLAUDE.md](./CLAUDE.md) for detailed architecture and development guidelines.

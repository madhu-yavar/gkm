# GKM Backend

FastAPI backend for the GKM tax return dashboard application.

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server (http://localhost:8000)
uvicorn app.main:app --reload

# Database migrations
alembic upgrade head
alembic revision --autogenerate -m "description"
```

## Environment

Copy `.env.example` to `.env` and configure:
```
DATABASE_URL=postgresql+psycopg2://gkm:gkm@localhost:5432/gkm
JWT_SECRET=change-me
SEED_ADMIN_EMAIL=admin@example.com
SEED_ADMIN_PASSWORD=admin1234
OPENAI_API_KEY=sk-...  # Optional
```

## Database

Start PostgreSQL with Docker:
```bash
docker-compose up -d
```

## Architecture

- **FastAPI**: REST API with automatic OpenAPI docs
- **SQLAlchemy**: ORM with session management
- **Alembic**: Database migration tool
- **JWT**: Token-based authentication with role-based access control
- **OpenPyXL**: Excel file parsing

## Key Routes

- `POST /auth/login` - User authentication
- `GET /snapshots` - List data snapshots
- `POST /documents/excel/{family}/preview` - Preview Excel structure
- `POST /documents/process` - Start async processing job
- `GET /dashboard/proposals` - Get/create dashboard blueprint proposals
- `GET /reports/summary.pdf` - Download overall PDF report

## What’s Implemented (MVP)

- JWT auth (seeded admin user on startup)
- Excel ingestion endpoint for `Contracted vs Actual -2026.xlsx` layout
- Snapshot-based dashboard APIs (KPIs, client table, staff list)
- PII detection and redaction
- Dashboard blueprint generation and proposals
- PDF report generation


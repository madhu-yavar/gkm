from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import models
from app.db import engine, session_scope
from app.routers import auth, dashboard, documents, snapshots
from app.security import hash_password
from app.settings import settings


def _seed_admin():
    with session_scope() as db:
        existing = db.query(models.User).filter(models.User.email == settings.seed_admin_email).first()
        if existing:
            return
        db.add(
            models.User(
                email=settings.seed_admin_email,
                password_hash=hash_password(settings.seed_admin_password),
                role=models.UserRole.admin,
                is_active=True,
            )
        )


def create_app() -> FastAPI:
    app = FastAPI(title="GKM API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(auth.router)
    app.include_router(documents.router)
    app.include_router(snapshots.router)
    app.include_router(dashboard.router)

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


app = create_app()


@app.on_event("startup")
def _on_startup():
    Path(settings.storage_dir).mkdir(parents=True, exist_ok=True)
    _seed_admin()

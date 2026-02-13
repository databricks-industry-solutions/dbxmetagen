"""SQLAlchemy engine, ORM models, and helpers for Lakebase (PostgreSQL) access."""

import os
import logging
from typing import Optional

from sqlalchemy import (
    create_engine, event, Column, String, Double, Boolean, DateTime, text,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine / session
# ---------------------------------------------------------------------------

_engine = None
_SessionLocal = None


_ws: WorkspaceClient | None = None


def _get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def _build_url() -> str:
    host = os.environ.get("PGHOST", "")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "")
    user = os.environ.get("PGUSER", "")
    sslmode = os.environ.get("PGSSLMODE", "require")
    # Password is injected per-connection via the do_connect event below
    return f"postgresql+psycopg2://{user}:@{host}:{port}/{db}?sslmode={sslmode}"


def pg_configured() -> bool:
    """Return True if the PG* env vars are set (Lakebase resource added)."""
    return bool(os.environ.get("PGHOST"))


def get_engine():
    global _engine
    if _engine is None:
        if not pg_configured():
            raise RuntimeError(
                "Lakebase not configured: PGHOST is empty. "
                "Add the Lakebase database as an app resource in the Databricks Apps UI."
            )
        url = _build_url()
        logger.info("Creating SQLAlchemy engine -> %s:%s/%s",
                     os.environ.get("PGHOST", "?"),
                     os.environ.get("PGPORT", "?"),
                     os.environ.get("PGDATABASE", "?"))
        _engine = create_engine(url, pool_pre_ping=True, pool_size=5)

        @event.listens_for(_engine, "do_connect")
        def _provide_token(dialect, conn_rec, cargs, cparams):
            cparams["password"] = _get_ws().config.oauth_token().access_token

    return _engine


def get_session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine())
    return _SessionLocal


def get_db():
    """FastAPI dependency that yields a SQLAlchemy session."""
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Raw SQL helper (for dynamic queries in traversal / agent tools)
# ---------------------------------------------------------------------------

def pg_execute(query: str, params: Optional[dict] = None) -> list[dict]:
    """Run a raw SQL query against Lakebase and return rows as list[dict].

    Returns [] if the table doesn't exist (graceful for pre-sync state).
    Raises HTTPException with a clear message if PG is not configured.
    """
    from fastapi import HTTPException

    if not pg_configured():
        raise HTTPException(
            500,
            detail="Lakebase not configured: add the Lakebase database as an app resource in the Databricks Apps UI.",
        )
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            cols = list(result.keys())
            return [dict(zip(cols, row)) for row in result.fetchall()]
    except HTTPException:
        raise
    except Exception as exc:
        msg = str(exc)
        if "does not exist" in msg or "UndefinedTable" in msg:
            logger.warning("PG table not found: %s", msg)
            raise HTTPException(404, detail=f"Lakebase table not found: {msg}") from exc
        logger.error("PG query error: %s", exc)
        raise HTTPException(500, detail=f"Lakebase query failed: {msg}") from exc


# ---------------------------------------------------------------------------
# ORM models (read-only mapping of existing Lakebase tables)
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class GraphNode(Base):
    __tablename__ = "graph_nodes"
    __table_args__ = {"schema": "public"}

    id = Column(String, primary_key=True)
    table_name = Column(String)
    catalog = Column(String)
    schema_ = Column("schema", String)
    table_short_name = Column(String)
    domain = Column(String)
    subdomain = Column(String)
    has_pii = Column(Boolean)
    has_phi = Column(Boolean)
    security_level = Column(String)
    comment = Column(String)
    node_type = Column(String)
    parent_id = Column(String)
    data_type = Column(String)
    quality_score = Column(Double)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class GraphEdge(Base):
    __tablename__ = "graph_edges"
    __table_args__ = {"schema": "public"}

    src = Column(String, primary_key=True)
    dst = Column(String, primary_key=True)
    relationship = Column(String, primary_key=True)
    weight = Column(Double)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

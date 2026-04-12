from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import os

# Import your existing modules
from models import Base
from config import settings

# ✅ IMPORTANT: this is what enables the debug route
from leaflink_debug import router as leaflink_debug_router


# =========================
# DATABASE SETUP
# =========================
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# =========================
# APP INIT
# =========================
app = FastAPI(
    title="Opsyn API",
    version="1.0.0",
    description="Full Ops Backend v1",
)

# ✅ THIS LINE FIXES YOUR 404
app.include_router(leaflink_debug_router)


# =========================
# STARTUP
# =========================
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# BASIC HEALTH CHECK
# =========================
@app.get("/")
def root():
    return {"status": "Opsyn backend running"}

@app.get("/health")
def health():
    return {"status": "healthy"}
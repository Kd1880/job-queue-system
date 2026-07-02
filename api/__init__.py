# api/__init__.py
# ------------------
# Marks `api` as a Python package so `uvicorn api.main:app` and
# `from api.database import ...`-style imports resolve correctly.
# Intentionally empty otherwise — no shared state belongs at package level.

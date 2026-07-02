# worker/__init__.py
# ------------------
# Marks `worker` as a Python package so `python -m worker.worker` and
# `from worker.job_handlers import ...`-style imports resolve correctly.
# Intentionally empty otherwise — no shared state belongs at package level.

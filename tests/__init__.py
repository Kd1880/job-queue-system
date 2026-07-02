# tests/__init__.py
# ------------------
# Marks `tests` as a Python package so pytest's rootdir-relative imports
# (e.g. `from api.models import ...`) resolve the same way whether tests
# are run from the repo root or via `python -m pytest`.

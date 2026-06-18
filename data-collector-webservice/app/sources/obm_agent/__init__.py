"""OBM agent ingest source.

Receives OpenText OBM (Operations Bridge Manager) agent metric payloads, maps
them to the canonical internal record shape, and produces Airflow staging files.
"""

from __future__ import annotations

from pathlib import Path

MAPPING_PATH = Path(__file__).resolve().parent / "mappings" / "collection_policy_summary.json"

# Logical name of this source; used in staging-file names and the `meta.source`
# field so rows in PostgreSQL are traceable back to their origin.
SOURCE_NAME = "obm_agent"

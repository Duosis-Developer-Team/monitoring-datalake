"""Data collector web service.

Receives monitoring metric payloads (OBM agents today, other sources later),
audits them, normalizes them, and writes Airflow-compatible staging files.
"""

__version__ = "0.1.0"

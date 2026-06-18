"""Ingest sources.

Each subpackage is a self-contained data source (its own normalizer, mapping,
staging metadata, and route). ``obm_agent`` is the first; future environments
plug in as sibling packages without touching the core services.
"""

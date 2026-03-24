"""
database
========
Phase 2 – Neo4j ETL pipeline components.

Provides the database connection layer and (future) ingestion logic
for loading enriched telecom graph data into Neo4j.

Public API
----------
Neo4jConnection      : Context-managed Neo4j driver wrapper.
GraphOntologyManager : Schema DDL manager (constraints & indexes).
"""

from .connection import Neo4jConnection
from .schema import GraphOntologyManager

__all__ = [
    "Neo4jConnection",
    "GraphOntologyManager",
]

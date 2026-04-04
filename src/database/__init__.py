"""
database
========
Phase 2 – Neo4j ETL pipeline components.

Provides the database connection layer and (future) ingestion logic
for loading enriched telecom graph data into Neo4j.

Public API
----------
Neo4jConnection        : Context-managed Neo4j driver wrapper.
GraphOntologyManager   : Schema DDL manager (constraints & indexes).
QuerySanitizer         : Validates and transforms LLM-generated Cypher.
SecurityViolationError : Raised when a query contains write keywords.
"""

from .connection import Neo4jConnection
from .schema import GraphOntologyManager
from .sanitizer import QuerySanitizer, SecurityViolationError

__all__ = [
    "Neo4jConnection",
    "GraphOntologyManager",
    "QuerySanitizer",
    "SecurityViolationError",
]

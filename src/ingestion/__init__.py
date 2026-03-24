"""
ingestion
=========
Phase 2 – CSV extraction, batching, and Neo4j loading utilities.

Provides memory-efficient, generator-based extraction of raw CSV data
produced by Phase 1 for downstream loading into Neo4j.

Public API
----------
CSVExtractor : Batch-yielding CSV reader.
DataLoader   : Cypher-based batch loader for nodes and edges.
"""

from .extractor import CSVExtractor
from .loader import DataLoader

__all__ = [
    "CSVExtractor",
    "DataLoader",
]

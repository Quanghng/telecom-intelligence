"""src.baseline – Phase 4 Baseline Vector RAG pipeline.

Provides a flat Vector RAG baseline (ChromaDB + sentence-transformers)
used as the *control* arm in our A/B evaluation against the GraphRAG
(Neo4j) approach.
"""

from .vector_etl import VectorETL
from .vector_retriever import VectorRetriever

__all__: list[str] = ["VectorETL", "VectorRetriever"]

"""
vector_retriever.py - VectorRetriever
======================================
Query interface for the baseline Vector RAG arm.

Connects to the persistent ChromaDB collection populated by
:class:`~src.baseline.vector_etl.VectorETL` and exposes a single
``retrieve_context`` method that embeds an arbitrary natural-language
query with the same ``all-MiniLM-L6-v2`` model and returns the *top-K*
most similar telecom documents.

Usage
-----
    >>> from src.baseline.vector_retriever import VectorRetriever
    >>> retriever = VectorRetriever()
    >>> docs = retriever.retrieve_context("Which nodes are Down?")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

import chromadb
from chromadb.api import ClientAPI
from chromadb.config import Settings as ChromaSettings
from pydantic import BaseModel, Field, field_validator

logger: logging.Logger = logging.getLogger(__name__)

# -- Defaults -----------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_DEFAULT_CHROMA_DIR: Final[Path] = _PROJECT_ROOT / "data" / "chroma_db"
_DEFAULT_COLLECTION: Final[str] = "telecom_docs"
_DEFAULT_MODEL: Final[str] = "all-MiniLM-L6-v2"


# -- Pydantic configuration model --------------------------------------------
class VectorRetrieverConfig(BaseModel):
    """Validated configuration for :class:`VectorRetriever`.

    Attributes
    ----------
    chroma_dir : Path
        Directory that holds the persistent ChromaDB storage.
    collection_name : str
        Name of the ChromaDB collection to query.
    embedding_model : str
        HuggingFace model identifier used by ``sentence-transformers``.
    """

    chroma_dir: Path = Field(default=_DEFAULT_CHROMA_DIR)
    collection_name: str = Field(default=_DEFAULT_COLLECTION)
    embedding_model: str = Field(default=_DEFAULT_MODEL)

    @field_validator("chroma_dir")
    @classmethod
    def _chroma_dir_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise FileNotFoundError(
                f"ChromaDB directory not found: {v}. "
                "Run VectorETL.run() first to build the vector store."
            )
        return v


class VectorRetriever:
    """Similarity-search retriever backed by a ChromaDB vector store.

    Parameters
    ----------
    config : VectorRetrieverConfig | None
        Optional pre-built config.  When *None* the class uses
        project-default paths.

    Raises
    ------
    FileNotFoundError
        If the ChromaDB persistence directory does not exist.
    ValueError
        If the expected collection is not found inside the database.

    Examples
    --------
    >>> retriever = VectorRetriever()
    >>> retriever.retrieve_context("Show all Degraded Cell_Tower nodes")
    ['TelecomNode physical Cell_Tower node_id 200 is currently Degraded.', ...]
    """

    def __init__(self, config: Optional[VectorRetrieverConfig] = None) -> None:
        self._cfg: VectorRetrieverConfig = config or VectorRetrieverConfig()

        # Lazy-import keeps the module importable even when the heavy
        # sentence-transformers wheel is not yet installed.
        from chromadb.utils.embedding_functions import (
            SentenceTransformerEmbeddingFunction,
        )

        self._embedding_fn = SentenceTransformerEmbeddingFunction(
            model_name=self._cfg.embedding_model,
        )

        self._client: ClientAPI = chromadb.PersistentClient(
            path=str(self._cfg.chroma_dir),
            settings=ChromaSettings(anonymized_telemetry=False),
        )

        # Validate that the collection actually exists.
        existing_names: List[str] = [
            c.name for c in self._client.list_collections()
        ]
        if self._cfg.collection_name not in existing_names:
            raise ValueError(
                f"Collection '{self._cfg.collection_name}' not found in "
                f"{self._cfg.chroma_dir}.  Available collections: "
                f"{existing_names}.  Run VectorETL.run() first."
            )

        self._collection = self._client.get_collection(
            name=self._cfg.collection_name,
            embedding_function=self._embedding_fn,  # type: ignore[arg-type]
        )

        logger.info(
            "VectorRetriever ready  |  collection='%s'  docs=%d",
            self._cfg.collection_name,
            self._collection.count(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve_context(
        self,
        query: str,
        top_k: int = 5,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Embed *query* and return the *top_k* most similar documents.

        Parameters
        ----------
        query : str
            Natural-language question or keyword phrase.
        top_k : int, optional
            Number of results to return (default ``5``).
        where : Dict[str, Any] | None, optional
            Optional ChromaDB ``where`` filter for metadata-level
            pre-filtering (e.g. ``{"layer": "physical"}``).

        Returns
        -------
        List[str]
            Up to *top_k* document strings ranked by cosine similarity.

        Raises
        ------
        ValueError
            If *query* is empty or *top_k* < 1.
        """
        if not query or not query.strip():
            raise ValueError("Query string must not be empty.")
        if top_k < 1:
            raise ValueError(f"top_k must be >= 1, got {top_k}.")

        logger.debug(
            "Querying ChromaDB: query=%r  top_k=%d  where=%s",
            query, top_k, where,
        )

        query_params: Dict[str, Any] = {
            "query_texts": [query],
            "n_results": min(top_k, self._collection.count()),
        }
        if where is not None:
            query_params["where"] = where

        results = self._collection.query(**query_params)

        raw_documents = results.get("documents")
        documents: List[str] = raw_documents[0] if raw_documents else []

        logger.info(
            "Retrieved %d document(s) for query: %r",
            len(documents),
            query[:80],
        )
        return documents

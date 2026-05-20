"""
vector_etl.py - VectorETL
==========================
Extract-Transform-Load pipeline for the baseline Vector RAG arm.

Reads the flattened graph data produced by Phase 1 (``data/raw/nodes.csv``
and ``data/raw/edges.csv``), converts every node and edge into a
human-readable textual document, embeds each document with
``sentence-transformers/all-MiniLM-L6-v2``, and persists the resulting
vectors in a local ChromaDB collection at ``data/chroma_db/``.

Usage
-----
    >>> from src.baseline.vector_etl import VectorETL
    >>> etl = VectorETL()
    >>> etl.run()
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, cast

import chromadb
from chromadb.api import ClientAPI
from chromadb.config import Settings as ChromaSettings
from pydantic import BaseModel, Field, field_validator

logger: logging.Logger = logging.getLogger(__name__)

# -- Defaults -----------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_DEFAULT_NODES_PATH: Final[Path] = _PROJECT_ROOT / "data" / "raw" / "nodes.csv"
_DEFAULT_EDGES_PATH: Final[Path] = _PROJECT_ROOT / "data" / "raw" / "edges.csv"
_DEFAULT_CHROMA_DIR: Final[Path] = _PROJECT_ROOT / "data" / "chroma_db"
_DEFAULT_COLLECTION: Final[str] = "telecom_docs"
_DEFAULT_MODEL: Final[str] = "all-MiniLM-L6-v2"
_BATCH_SIZE: Final[int] = 256

# -- Relationship-type defaults (mirrors loader.py convention) ----------------
_DEFAULT_REL_BY_LAYER: Final[Dict[str, str]] = {
    "physical": "CONNECTS_TO",
    "logical": "RUNS_ON",
    "service": "DEPENDS_ON",
    "business": "SUBSCRIBED_TO",
}


# -- Pydantic configuration model --------------------------------------------
class VectorETLConfig(BaseModel):
    """Validated configuration for :class:`VectorETL`.

    All paths default to the standard project layout so that
    ``VectorETL()`` works out-of-the-box after Phase 1 data generation.
    """

    nodes_path: Path = Field(default=_DEFAULT_NODES_PATH)
    edges_path: Path = Field(default=_DEFAULT_EDGES_PATH)
    chroma_dir: Path = Field(default=_DEFAULT_CHROMA_DIR)
    collection_name: str = Field(default=_DEFAULT_COLLECTION)
    embedding_model: str = Field(default=_DEFAULT_MODEL)
    batch_size: int = Field(default=_BATCH_SIZE, ge=1)

    @field_validator("nodes_path", "edges_path")
    @classmethod
    def _path_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise FileNotFoundError(
                f"Required data file not found: {v}. "
                "Run Phase 1 data generation first "
                "(python -m src.data_generation)."
            )
        return v


class VectorETL:
    """ETL pipeline: CSV -> text documents -> ChromaDB vector store.

    Parameters
    ----------
    config : VectorETLConfig | None
        Optional pre-built config.  When *None* the class uses
        project-default paths.

    Examples
    --------
    >>> etl = VectorETL()
    >>> etl.run()
    """

    def __init__(self, config: Optional[VectorETLConfig] = None) -> None:
        self._cfg: VectorETLConfig = config or VectorETLConfig()
        self._documents: List[str] = []
        self._ids: List[str] = []
        self._metadatas: List[Dict[str, str]] = []
        logger.info(
            "VectorETL initialised  |  nodes=%s  edges=%s  chroma_dir=%s",
            self._cfg.nodes_path,
            self._cfg.edges_path,
            self._cfg.chroma_dir,
        )

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    @staticmethod
    def _read_csv(path: Path) -> List[Dict[str, str]]:
        """Read a CSV file and return a list of row dictionaries.

        Parameters
        ----------
        path : Path
            Absolute path to the CSV file.

        Returns
        -------
        List[Dict[str, str]]
            Each dictionary maps column header -> cell value.
        """
        with open(path, mode="r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows: List[Dict[str, str]] = list(reader)
        logger.info("Read %d rows from %s", len(rows), path.name)
        return rows

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def _node_to_document(self, row: Dict[str, str]) -> str:
        """Convert a single node row into a descriptive text document.

        The text intentionally includes all schema-relevant fields so
        that semantic similarity can surface relevant nodes for natural-
        language queries.

        Parameters
        ----------
        row : Dict[str, str]
            A single row from ``nodes.csv``.

        Returns
        -------
        str
            Human-readable textual representation of the node.
        """
        node_id: str = row.get("node_id", "unknown")
        layer: str = row.get("layer", "unknown")
        node_type: str = row.get("node_type", "unknown")
        status: str = row.get("status", "unknown")
        name: str = row.get("name", "").strip()
        sla: str = row.get("sla", "").strip()
        cpu: str = row.get("cpu_utilization", "").strip()

        parts: List[str] = [
            f"TelecomNode {layer} {node_type} node_id {node_id}",
            f"is currently {status}.",
        ]
        if name:
            parts.insert(1, f"named {name}")
        if sla:
            parts.append(f"SLA tier: {sla}.")
        if cpu:
            parts.append(f"CPU utilization: {cpu}%.")

        return " ".join(parts)

    def _edge_to_document(self, row: Dict[str, str]) -> str:
        """Convert a single edge row into a descriptive text document.

        Parameters
        ----------
        row : Dict[str, str]
            A single row from ``edges.csv``.

        Returns
        -------
        str
            Human-readable textual representation of the edge.
        """
        source: str = row.get("source", "unknown")
        target: str = row.get("target", "unknown")
        layer: str = row.get("layer", "unknown")
        rel: str = (row.get("relationship") or "").strip()
        if not rel:
            rel = _DEFAULT_REL_BY_LAYER.get(layer, "RELATED_TO")
        status: str = row.get("status", "").strip()
        capacity: str = row.get("capacity_gbps", "").strip()
        latency: str = row.get("latency_ms", "").strip()
        pkt_loss: str = row.get("packet_loss_pct", "").strip()

        parts: List[str] = [
            f"Relationship {rel} from node {source} to node {target}",
            f"on the {layer} layer.",
        ]
        if capacity:
            parts.append(f"Capacity: {capacity} Gbps.")
        if latency:
            parts.append(f"Latency: {latency} ms.")
        if pkt_loss:
            parts.append(f"Packet loss: {pkt_loss}%.")
        if status:
            parts.append(f"Link status: {status}.")

        return " ".join(parts)

    def _transform(self) -> None:
        """Read both CSVs and build the parallel document / id / metadata lists."""
        node_rows: List[Dict[str, str]] = self._read_csv(self._cfg.nodes_path)
        edge_rows: List[Dict[str, str]] = self._read_csv(self._cfg.edges_path)

        for row in node_rows:
            doc: str = self._node_to_document(row)
            doc_id: str = f"node_{row.get('node_id', 'unknown')}"
            meta: Dict[str, str] = {
                "source_type": "node",
                "layer": row.get("layer", "unknown"),
                "node_type": row.get("node_type", "unknown"),
                "status": row.get("status", "unknown"),
            }
            self._documents.append(doc)
            self._ids.append(doc_id)
            self._metadatas.append(meta)

        for idx, row in enumerate(edge_rows):
            doc = self._edge_to_document(row)
            doc_id = f"edge_{row.get('source', 'x')}_{row.get('target', 'y')}_{idx}"
            rel: str = (row.get("relationship") or "").strip()
            if not rel:
                rel = _DEFAULT_REL_BY_LAYER.get(
                    row.get("layer", ""), "RELATED_TO"
                )
            meta = {
                "source_type": "edge",
                "layer": row.get("layer", "unknown"),
                "relationship": rel,
                "status": row.get("status", "").strip() or "up",
            }
            self._documents.append(doc)
            self._ids.append(doc_id)
            self._metadatas.append(meta)

        logger.info(
            "Transformed %d documents  (%d nodes, %d edges).",
            len(self._documents),
            len(node_rows),
            len(edge_rows),
        )

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Embed documents and upsert them into a persistent ChromaDB collection.

        Uses the ``sentence-transformers/all-MiniLM-L6-v2`` model via
        ChromaDB's built-in ``SentenceTransformerEmbeddingFunction``.
        Documents are sent in batches to keep memory usage predictable.
        """
        from chromadb.utils.embedding_functions import (
            SentenceTransformerEmbeddingFunction,
        )

        self._cfg.chroma_dir.mkdir(parents=True, exist_ok=True)

        client: ClientAPI = chromadb.PersistentClient(
            path=str(self._cfg.chroma_dir),
            settings=ChromaSettings(anonymized_telemetry=False),
        )

        embedding_fn = SentenceTransformerEmbeddingFunction(
            model_name=self._cfg.embedding_model,
        )

        # get_or_create ensures idempotent re-runs.
        # NOTE: chromadb's EmbeddingFunction generic is contravariant on
        # its type-param which makes SentenceTransformerEmbeddingFunction
        # technically incompatible at the type level - safe at runtime.
        collection = client.get_or_create_collection(
            name=self._cfg.collection_name,
            embedding_function=embedding_fn,  # type: ignore[arg-type]
            metadata={"hnsw:space": "cosine"},
        )

        total: int = len(self._documents)
        for start in range(0, total, self._cfg.batch_size):
            end: int = min(start + self._cfg.batch_size, total)
            collection.upsert(
                ids=self._ids[start:end],
                documents=self._documents[start:end],
                metadatas=cast(Any, self._metadatas[start:end]),
            )
            logger.info(
                "Upserted batch [%d - %d] / %d into ChromaDB.",
                start,
                end - 1,
                total,
            )

        logger.info(
            "ChromaDB collection '%s' now contains %d documents.",
            self._cfg.collection_name,
            collection.count(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the full ETL pipeline: extract -> transform -> load.

        Raises
        ------
        FileNotFoundError
            If the expected CSV files do not exist on disk.
        chromadb.errors.ChromaError
            On any ChromaDB persistence or embedding failure.
        """
        logger.info("VectorETL pipeline started.")
        self._transform()
        self._load()
        logger.info("VectorETL pipeline completed successfully.")

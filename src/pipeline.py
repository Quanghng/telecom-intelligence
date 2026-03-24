"""
pipeline.py – Neo4jETLPipeline
===============================
Top-level orchestrator for the Phase 2 Telecom GraphRAG ETL pipeline.

Coordinates the full extract → schema → load workflow:

1. Opens a managed Neo4j connection.
2. Ensures all constraints and indexes exist via :class:`GraphOntologyManager`.
3. Streams node and edge CSV files in fixed-size batches via :class:`CSVExtractor`.
4. Merges each batch into Neo4j via :class:`DataLoader`.

Usage
-----
Run directly from the project root::

    python -m src.pipeline

Or import and invoke programmatically::

    >>> from pathlib import Path
    >>> from src.pipeline import Neo4jETLPipeline
    >>> Neo4jETLPipeline(
    ...     nodes_path=Path("data/raw/nodes.csv"),
    ...     edges_path=Path("data/raw/edges.csv"),
    ... ).run()
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.database.connection import Neo4jConnection
from src.database.schema import GraphOntologyManager
from src.ingestion.extractor import CSVExtractor
from src.ingestion.loader import DataLoader

logger: logging.Logger = logging.getLogger(__name__)


class Neo4jETLPipeline:
    """End-to-end ETL pipeline: CSV → Neo4j.

    Parameters
    ----------
    nodes_path : Path
        Filesystem path to the nodes CSV file.
    edges_path : Path
        Filesystem path to the edges CSV file.
    batch_size : int, optional
        Maximum number of rows per batch handed to the loader
        (default ``2000``).

    Examples
    --------
    >>> Neo4jETLPipeline(
    ...     nodes_path=Path("data/raw/nodes.csv"),
    ...     edges_path=Path("data/raw/edges.csv"),
    ...     batch_size=1000,
    ... ).run()
    """

    def __init__(
        self,
        nodes_path: Path,
        edges_path: Path,
        batch_size: int = 2000,
    ) -> None:
        self._nodes_path: Path = nodes_path
        self._edges_path: Path = edges_path
        self._batch_size: int = batch_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the full ETL pipeline.

        Workflow
        --------
        1. Open a context-managed :class:`Neo4jConnection`.
        2. Apply schema DDL (constraints + indexes).
        3. Stream and load **nodes** in batches.
        4. Stream and load **edges** in batches.

        Raises
        ------
        RuntimeError
            If the Neo4j driver cannot be initialised.
        neo4j.exceptions.Neo4jError
            Propagated from any failed Cypher statement.
        FileNotFoundError
            If a CSV path does not exist on disk.
        """
        logger.info(
            "Pipeline started  ➜  nodes=%s | edges=%s | batch_size=%d",
            self._nodes_path,
            self._edges_path,
            self._batch_size,
        )

        with Neo4jConnection() as conn:
            # ── 1. Schema ────────────────────────────────────────────
            logger.info("Phase 1/3 — Setting up schema …")
            schema_manager: GraphOntologyManager = GraphOntologyManager(conn)
            schema_manager.setup_schema()

            # ── 2. Shared utilities ──────────────────────────────────
            extractor: CSVExtractor = CSVExtractor()
            loader: DataLoader = DataLoader(conn)

            # ── 3. Nodes ─────────────────────────────────────────────
            logger.info("Phase 2/3 — Loading nodes from %s …", self._nodes_path)
            node_batch_count: int = 0
            for batch in extractor.extract_in_batches(
                str(self._nodes_path), self._batch_size
            ):
                loader.load_nodes(batch)
                node_batch_count += 1
            logger.info(
                "Node loading complete — %d batch(es) processed.", node_batch_count
            )

            # ── 4. Edges ─────────────────────────────────────────────
            logger.info("Phase 3/3 — Loading edges from %s …", self._edges_path)
            edge_batch_count: int = 0
            for batch in extractor.extract_in_batches(
                str(self._edges_path), self._batch_size
            ):
                loader.load_edges(batch)
                edge_batch_count += 1
            logger.info(
                "Edge loading complete — %d batch(es) processed.", edge_batch_count
            )

        logger.info("Pipeline finished successfully. ✔")


# ------------------------------------------------------------------
# CLI entry-point
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    )

    # Resolve paths relative to the project root (parent of src/).
    project_root: Path = Path(__file__).resolve().parent.parent
    nodes_csv: Path = project_root / "data" / "raw" / "nodes.csv"
    edges_csv: Path = project_root / "data" / "raw" / "edges.csv"

    pipeline: Neo4jETLPipeline = Neo4jETLPipeline(
        nodes_path=nodes_csv,
        edges_path=edges_csv,
    )
    pipeline.run()

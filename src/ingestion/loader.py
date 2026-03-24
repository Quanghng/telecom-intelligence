"""
loader.py – DataLoader
=======================
Cypher-based batch loader for the Telecom GraphRAG ETL pipeline.

Groups raw CSV rows by their Neo4j label / relationship type and
executes parameterized ``UNWIND … MERGE`` statements through the
:class:`~src.database.connection.Neo4jConnection` wrapper.

Because Neo4j does **not** allow parameterized labels or relationship
types, those identifiers are injected via f-strings while all *data*
values remain safely parameterized.

Usage
-----
    >>> from src.database.connection import Neo4jConnection
    >>> from src.ingestion.loader import DataLoader
    >>> with Neo4jConnection() as conn:
    ...     loader = DataLoader(conn)
    ...     loader.load_nodes(node_batch)
    ...     loader.load_edges(edge_batch)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, Final, List, Tuple, cast
from typing_extensions import LiteralString

from src.database.connection import Neo4jConnection

logger: logging.Logger = logging.getLogger(__name__)

# Fallback relationship types when the CSV ``relationship`` column is
# empty.  Keyed by the ``layer`` value of the edge row.
_DEFAULT_REL_BY_LAYER: Final[Dict[str, str]] = {
    "physical": "CONNECTS_TO",
    "logical": "RUNS_ON",
    "service": "DEPENDS_ON",
    "business": "SUBSCRIBED_TO",
}
_FALLBACK_REL: Final[str] = "RELATED_TO"


class DataLoader:
    """Batch loader that writes nodes and edges to Neo4j via Cypher.

    Parameters
    ----------
    connection : Neo4jConnection
        An **already-initialised** (context-managed) Neo4j connection
        used to execute all Cypher statements.

    Examples
    --------
    >>> with Neo4jConnection() as conn:
    ...     loader = DataLoader(conn)
    ...     loader.load_nodes(node_batch)
    ...     loader.load_edges(edge_batch)
    """

    def __init__(self, connection: Neo4jConnection) -> None:
        self._connection: Neo4jConnection = connection

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_label(raw: str) -> str:
        """Backtick-escape a label/type so Neo4j accepts any characters.

        Neo4j requires backtick quoting for identifiers that start with
        a digit or contain characters outside ``[a-zA-Z0-9_]``.
        Wrapping unconditionally is safe and idempotent.
        """
        return f"`{raw}`"

    @staticmethod
    def _resolve_relationship(row: Dict[str, Any]) -> str:
        """Return a non-empty relationship type for *row*.

        If the ``relationship`` field is present and non-blank it is
        returned as-is.  Otherwise a sensible default is derived from
        the ``layer`` field.
        """
        rel: str = (row.get("relationship") or "").strip()
        if rel:
            return rel
        layer: str = (row.get("layer") or "").strip().lower()
        return _DEFAULT_REL_BY_LAYER.get(layer, _FALLBACK_REL)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_nodes(self, batch: List[Dict[str, Any]]) -> None:
        """Merge a batch of node dictionaries into Neo4j.

        Each dictionary **must** contain at least the keys ``node_id``,
        ``layer``, and ``node_type``.  Rows are grouped by
        ``(layer, node_type)`` so that a single ``UNWIND`` query can
        apply the correct pair of labels to every node in the group.

        Parameters
        ----------
        batch : List[Dict[str, Any]]
            Flat list of node dictionaries extracted from a CSV file.

        Raises
        ------
        KeyError
            If a row is missing ``layer`` or ``node_type``.
        neo4j.exceptions.Neo4jError
            Propagated from the underlying driver on query failure.
        """
        if not batch:
            logger.warning("load_nodes called with an empty batch – skipping.")
            return

        # Group rows by (layer, node_type) for label injection.
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in batch:
            key: Tuple[str, str] = (row["layer"], row["node_type"])
            groups[key].append(row)

        for (layer, node_type), rows in groups.items():
            safe_layer: str = self._safe_label(layer)
            safe_type: str = self._safe_label(node_type)
            query: str = (
                f"UNWIND $rows AS row "
                f"MERGE (n:TelecomNode {{node_id: row.node_id}}) "
                f"SET n :{safe_layer}:{safe_type}, n += row"
            )
            self._connection.execute_query(cast(LiteralString, query), {"rows": rows})
            logger.info(
                "Loaded %d node(s) with labels [TelecomNode, %s, %s].",
                len(rows),
                layer,
                node_type,
            )

    def load_edges(self, batch: List[Dict[str, Any]]) -> None:
        """Merge a batch of edge dictionaries into Neo4j.

        Each dictionary **must** contain at least the keys ``source``
        and ``target``.  The ``relationship`` field is used as the
        Neo4j relationship type; when it is missing or blank a
        sensible default is inferred from the ``layer`` field.

        Rows are grouped by relationship type so that a single
        ``UNWIND`` query handles each group.

        Parameters
        ----------
        batch : List[Dict[str, Any]]
            Flat list of edge dictionaries extracted from a CSV file.

        Raises
        ------
        neo4j.exceptions.Neo4jError
            Propagated from the underlying driver on query failure.
        """
        if not batch:
            logger.warning("load_edges called with an empty batch – skipping.")
            return

        # Group rows by relationship type for type injection.
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in batch:
            rel: str = self._resolve_relationship(row)
            groups[rel].append(row)

        for rel_type, rows in groups.items():
            safe_rel: str = self._safe_label(rel_type)
            query: str = (
                f"UNWIND $rows AS row "
                f"MATCH (src:TelecomNode {{node_id: row.source}}) "
                f"MATCH (tgt:TelecomNode {{node_id: row.target}}) "
                f"MERGE (src)-[r:{safe_rel}]->(tgt) "
                f"SET r += row"
            )
            self._connection.execute_query(cast(LiteralString, query), {"rows": rows})
            logger.info(
                "Loaded %d edge(s) of type [%s].",
                len(rows),
                rel_type,
            )

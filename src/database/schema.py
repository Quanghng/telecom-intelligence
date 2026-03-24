"""
schema.py – GraphOntologyManager
==================================
Manages the Neo4j schema layer for the Telecom GraphRAG system.

Responsible for creating uniqueness constraints and secondary indexes
that enforce data integrity and accelerate Cypher lookups during
the RAG retrieval phase.

Usage
-----
    >>> from src.database import Neo4jConnection
    >>> from src.database.schema import GraphOntologyManager
    >>> with Neo4jConnection() as conn:
    ...     manager = GraphOntologyManager(conn)
    ...     manager.setup_schema()
"""

from __future__ import annotations

import logging
from typing import Final
from typing_extensions import LiteralString

from neo4j.exceptions import Neo4jError

from src.database.connection import Neo4jConnection

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Cypher DDL statements (Neo4j 5+ syntax)
# ------------------------------------------------------------------

_CONSTRAINT_TELECOM_NODE_ID: Final[LiteralString] = (
    "CREATE CONSTRAINT telecom_node_id IF NOT EXISTS "
    "FOR (n:TelecomNode) REQUIRE n.node_id IS UNIQUE"
)

_INDEX_TELECOM_NODE_STATUS: Final[LiteralString] = (
    "CREATE INDEX telecom_node_status IF NOT EXISTS "
    "FOR (n:TelecomNode) ON (n.status)"
)

_INDEX_TELECOM_NODE_NAME: Final[LiteralString] = (
    "CREATE INDEX telecom_node_name IF NOT EXISTS "
    "FOR (n:TelecomNode) ON (n.name)"
)


class GraphOntologyManager:
    """Creates and maintains the Neo4j schema for the telecom ontology.

    This class owns every DDL statement (constraints, indexes) that the
    ETL pipeline requires.  It receives a live :class:`Neo4jConnection`
    via dependency injection so it can be tested with a stub or a
    disposable database instance.

    Parameters
    ----------
    connection : Neo4jConnection
        An **already-entered** context-managed connection (i.e. the
        object returned by ``Neo4jConnection().__enter__()``).

    Examples
    --------
    >>> with Neo4jConnection() as conn:
    ...     GraphOntologyManager(conn).setup_schema()
    """

    def __init__(self, connection: Neo4jConnection) -> None:
        self._connection: Neo4jConnection = connection
        logger.info("GraphOntologyManager initialised.")

    # ------------------------------------------------------------------
    # Constraints
    # ------------------------------------------------------------------

    def create_constraints(self) -> None:
        """Create uniqueness constraints on core telecom node labels.

        Executes
        --------
        ``CREATE CONSTRAINT telecom_node_id IF NOT EXISTS
        FOR (n:TelecomNode) REQUIRE n.node_id IS UNIQUE``

        The ``IF NOT EXISTS`` clause makes the operation idempotent,
        so it is safe to call repeatedly.
        """
        logger.info("Creating uniqueness constraints …")
        self._connection.execute_query(_CONSTRAINT_TELECOM_NODE_ID)
        logger.info(
            "Constraint 'telecom_node_id' ensured (node_id UNIQUE on :TelecomNode)."
        )

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def create_indexes(self) -> None:
        """Create secondary B-tree indexes for frequently filtered properties.

        Executes
        --------
        * ``CREATE INDEX telecom_node_status IF NOT EXISTS
          FOR (n:TelecomNode) ON (n.status)``
        * ``CREATE INDEX telecom_node_name IF NOT EXISTS
          FOR (n:TelecomNode) ON (n.name)``

        These indexes accelerate ``WHERE n.status = …`` and
        ``WHERE n.name = …`` filters used heavily by the GraphRAG
        retrieval layer.
        """
        logger.info("Creating secondary indexes …")

        self._connection.execute_query(_INDEX_TELECOM_NODE_STATUS)
        logger.info("Index 'telecom_node_status' ensured (status on :TelecomNode).")

        self._connection.execute_query(_INDEX_TELECOM_NODE_NAME)
        logger.info("Index 'telecom_node_name' ensured (name on :TelecomNode).")

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def setup_schema(self) -> None:
        """Run the full schema setup: constraints first, then indexes.

        This is the single entry-point that the ETL pipeline should
        call.  Each phase is wrapped in its own ``try / except`` block
        so that a failure in one phase is logged clearly without
        masking the other.

        Raises
        ------
        Neo4jError
            Re-raised after logging if a DDL statement fails.
        """
        logger.info("Starting full schema setup …")

        try:
            self.create_constraints()
        except Neo4jError as exc:
            logger.error("Schema setup failed during constraint creation: %s", exc)
            raise

        try:
            self.create_indexes()
        except Neo4jError as exc:
            logger.error("Schema setup failed during index creation: %s", exc)
            raise

        logger.info("Schema setup completed successfully.")

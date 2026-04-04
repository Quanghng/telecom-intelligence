"""
connection.py - Neo4jConnection
================================
Context-managed wrapper around the official Neo4j Python driver.

Provides ``execute_query`` for general Cypher execution and
``execute_llm_read_query`` for **Phase 3 GraphRAG** read operations
that are automatically sanitised before they reach the database.

Usage
-----
    >>> from src.database import Neo4jConnection
    >>> with Neo4jConnection() as conn:
    ...     rows = conn.execute_query("MATCH (n) RETURN n LIMIT $limit", {"limit": 5})
    ...     safe = conn.execute_llm_read_query("MATCH (n) RETURN n.name")
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Dict, List, Optional, Type
from typing_extensions import LiteralString

from neo4j import GraphDatabase, Driver, ManagedTransaction, Session
from neo4j.exceptions import Neo4jError

from config.settings import Neo4jSettings, neo4j_settings
from src.database.sanitizer import QuerySanitizer, SecurityViolationError

logger: logging.Logger = logging.getLogger(__name__)

# Module-level sanitizer instance (stateless, safe to share).
_sanitizer = QuerySanitizer()


class Neo4jConnection:
    """Context-managed Neo4j driver wrapper.

    Parameters
    ----------
    settings : Neo4jSettings, optional
        Connection parameters.  Falls back to the module-level
        ``neo4j_settings`` singleton when not supplied.

    Examples
    --------
    >>> with Neo4jConnection() as conn:
    ...     result = conn.execute_query(
    ...         "MATCH (n:Router) RETURN n.name AS name LIMIT $k",
    ...         {"k": 10},
    ...     )
    ...     safe = conn.execute_llm_read_query(
    ...         "MATCH (n:Router) RETURN n.name",
    ...     )
    """

    def __init__(self, settings: Optional[Neo4jSettings] = None) -> None:
        self._settings: Neo4jSettings = settings or neo4j_settings
        self._driver: Optional[Driver] = None
        logger.info(
            "Initialising Neo4jConnection -> %s (database=%s)",
            self._settings.uri,
            self._settings.database,
        )

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "Neo4jConnection":
        """Create the Neo4j driver and verify connectivity."""
        self._driver = GraphDatabase.driver(
            self._settings.uri,
            auth=(self._settings.user, self._settings.password.get_secret_value()),
        )
        try:
            self._driver.verify_connectivity()
            logger.info("Neo4j connectivity verified successfully.")
        except Neo4jError as exc:
            logger.error("Failed to verify Neo4j connectivity: %s", exc)
            self.close()
            raise
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Close the driver, releasing all pooled connections."""
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_query(
        self,
        query: LiteralString,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Run a parameterized Cypher query and return results.

        Parameters
        ----------
        query : LiteralString
            A Cypher query string.  **Must** use ``$param`` placeholders
            instead of string formatting to prevent injection.
        parameters : Dict[str, Any], optional
            Mapping of parameter names to values.

        Returns
        -------
        List[Dict[str, Any]]
            Each element is one record, represented as a dictionary
            keyed by the RETURN aliases.

        Raises
        ------
        RuntimeError
            If the driver has not been initialised (i.e. used outside a
            ``with`` block).
        neo4j.exceptions.Neo4jError
            Re-raised after logging any driver- or server-side error.
        """
        if self._driver is None:
            raise RuntimeError(
                "Driver not initialised. Use Neo4jConnection as a context manager."
            )

        logger.debug("Executing query: %s | params: %s", query, parameters)

        session: Session = self._driver.session(
            database=self._settings.database,
        )
        try:
            result = session.run(query, parameters or {})
            records: List[Dict[str, Any]] = [record.data() for record in result]
            logger.debug("Query returned %d record(s).", len(records))
            return records
        except Neo4jError as exc:
            logger.error(
                "Neo4jError while executing query.\n  Query : %s\n  Params: %s\n  Error : %s",
                query,
                parameters,
                exc,
            )
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Phase 3 - GraphRAG read-only query path
    # ------------------------------------------------------------------

    def execute_llm_read_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute an LLM-generated Cypher query in a **read-only** transaction.

        The query passes through two sanitisation gates before execution:

        1. ``QuerySanitizer.validate_read_only`` -- raises
           ``SecurityViolationError`` if any write keyword is detected.
        2. ``QuerySanitizer.enforce_limits`` -- appends a ``LIMIT 50``
           clause when the query is unbounded, preventing context-window
           overflow.

        Uses ``session.execute_read()`` so the Neo4j driver will
        **never** route this query to a write transaction.

        Parameters
        ----------
        query : str
            A Cypher query string produced by the LLM.
        params : Dict[str, Any], optional
            Mapping of parameter names to values.

        Returns
        -------
        List[Dict[str, Any]]
            Each element is one record as a dictionary keyed by the
            RETURN aliases.

        Raises
        ------
        RuntimeError
            If the driver has not been initialised.
        SecurityViolationError
            If the query contains a write/mutation keyword.
        neo4j.exceptions.Neo4jError
            Re-raised after logging any driver- or server-side error.
        """
        if self._driver is None:
            raise RuntimeError(
                "Driver not initialised. Use Neo4jConnection as a context manager."
            )

        logger.info("LLM read query received: %s | params: %s", query, params)

        # -- Gate 1: block write / mutation keywords ---------------
        try:
            _sanitizer.validate_read_only(query)
        except SecurityViolationError:
            logger.error(
                "LLM query rejected by read-only gate: %s", query,
            )
            raise

        # -- Gate 2: enforce a result-set LIMIT --------------------
        safe_query: str = _sanitizer.enforce_limits(query)
        if safe_query != query:
            logger.info(
                "LLM query rewritten by enforce_limits:\n"
                "  original : %s\n  sanitised: %s",
                query,
                safe_query,
            )

        # -- Execute inside an explicit read transaction -----------
        resolved_params: Dict[str, Any] = params or {}

        def _read_tx(tx: ManagedTransaction) -> List[Dict[str, Any]]:
            result = tx.run(safe_query, resolved_params)  # type: ignore[arg-type]
            return [record.data() for record in result]

        session: Session = self._driver.session(
            database=self._settings.database,
        )
        try:
            records = session.execute_read(_read_tx)
            logger.info(
                "LLM read query returned %d record(s).", len(records),
            )
            return records
        except Neo4jError as exc:
            logger.error(
                "Neo4jError during LLM read query.\n"
                "  Query : %s\n  Params: %s\n  Error : %s",
                safe_query,
                resolved_params,
                exc,
            )
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Explicitly close the underlying driver.

        Safe to call multiple times or when the driver is already
        ``None``.
        """
        if self._driver is not None:
            self._driver.close()
            logger.info("Neo4j driver closed.")
            self._driver = None


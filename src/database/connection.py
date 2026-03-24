"""
connection.py – Neo4jConnection
================================
Context-managed wrapper around the official Neo4j Python driver.

Provides a single ``execute_query`` method that opens a session, runs a
parameterized Cypher query, and returns results as a list of dictionaries.

Usage
-----
    >>> from src.database import Neo4jConnection
    >>> with Neo4jConnection() as conn:
    ...     rows = conn.execute_query("MATCH (n) RETURN n LIMIT $limit", {"limit": 5})
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Dict, List, Optional, Type
from typing_extensions import LiteralString

from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import Neo4jError

from config.settings import Neo4jSettings, neo4j_settings

logger: logging.Logger = logging.getLogger(__name__)


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
    """

    def __init__(self, settings: Optional[Neo4jSettings] = None) -> None:
        self._settings: Neo4jSettings = settings or neo4j_settings
        self._driver: Optional[Driver] = None
        logger.info(
            "Initialising Neo4jConnection → %s (database=%s)",
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
            auth=(self._settings.user, self._settings.password),
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

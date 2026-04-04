"""
sanitizer.py – QuerySanitizer
===============================
Guards for LLM-generated Cypher queries in the GraphRAG pipeline.

Responsibilities
----------------
* **Read-only enforcement** – block any query that contains Cypher
  write/mutation keywords before it ever reaches the database driver.
* **Result-set limiting** – automatically append a ``LIMIT`` clause to
  unbounded queries so large result sets don't overflow the LLM context
  window.

Usage
-----
    >>> from src.database.sanitizer import QuerySanitizer
    >>> qs = QuerySanitizer()
    >>> qs.validate_read_only("MATCH (n) RETURN n")    # True
    >>> qs.enforce_limits("MATCH (n) RETURN n")         # 'MATCH (n) RETURN n LIMIT 50'
"""

from __future__ import annotations

import logging
import re

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled patterns (module-level to avoid recompilation on every call)
# ---------------------------------------------------------------------------

# Matches Cypher write / mutation keywords as whole words, case-insensitive.
# Uses word-boundary anchors so that column names like "created_at" are safe.
_WRITE_KEYWORDS_RE: re.Pattern[str] = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP)\b",
    re.IGNORECASE,
)

# Detects an existing LIMIT clause at the tail of a query
# (optional whitespace / semicolon afterwards).
_LIMIT_TAIL_RE: re.Pattern[str] = re.compile(
    r"\bLIMIT\s+(\$\w+|\d+)\s*;?\s*$",
    re.IGNORECASE,
)

# Common Cypher aggregation functions whose output is inherently bounded.
_AGGREGATION_RE: re.Pattern[str] = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|COLLECT)\s*\(",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class SecurityViolationError(Exception):
    """Raised when a Cypher query contains forbidden write/mutation keywords.

    Attributes
    ----------
    query : str
        The offending query string.
    keyword : str
        The matched write keyword that triggered the violation.
    """

    def __init__(self, query: str, keyword: str) -> None:
        self.query = query
        self.keyword = keyword
        super().__init__(
            f"Write operation blocked — forbidden keyword '{keyword}' "
            f"detected in query: {query!r}"
        )


# ---------------------------------------------------------------------------
# Sanitizer
# ---------------------------------------------------------------------------

class QuerySanitizer:
    """Validates and transforms LLM-generated Cypher before execution.

    Examples
    --------
    >>> qs = QuerySanitizer()
    >>> qs.validate_read_only("MATCH (n:Router) RETURN n.name")
    True
    >>> qs.enforce_limits("MATCH (n:Router) RETURN n.name")
    'MATCH (n:Router) RETURN n.name LIMIT 50'
    """

    # ------------------------------------------------------------------
    # Read-only gate
    # ------------------------------------------------------------------

    @staticmethod
    def validate_read_only(query: str) -> bool:
        """Check that *query* contains **no** Cypher write keywords.

        Parameters
        ----------
        query : str
            A Cypher query string to inspect.

        Returns
        -------
        bool
            ``True`` when the query is purely read-only.

        Raises
        ------
        SecurityViolationError
            If any write/mutation keyword (``CREATE``, ``MERGE``,
            ``DELETE``, ``SET``, ``REMOVE``, ``DROP``) is found.
        """
        match = _WRITE_KEYWORDS_RE.search(query)
        if match:
            keyword = match.group(0).upper()
            logger.warning(
                "BLOCKED — write keyword '%s' found in query: %s",
                keyword,
                query,
            )
            raise SecurityViolationError(query=query, keyword=keyword)

        logger.debug("Query passed read-only validation: %s", query)
        return True

    # ------------------------------------------------------------------
    # Limit enforcement
    # ------------------------------------------------------------------

    def enforce_limits(
        self,
        query: str,
        default_limit: int = 50,
    ) -> str:
        """Append a ``LIMIT`` clause when the query is unbounded.

        A query is considered *already bounded* when it:

        * ends with a ``LIMIT <n>`` or ``LIMIT $param`` clause, **or**
        * uses a Cypher aggregation function (``COUNT``, ``SUM``,
          ``AVG``, ``MIN``, ``MAX``, ``COLLECT``).

        Parameters
        ----------
        query : str
            A Cypher query string.
        default_limit : int, optional
            The row cap to append when no limit is present (default 50).

        Returns
        -------
        str
            The (possibly modified) query string.
        """
        stripped = query.rstrip().rstrip(";")

        # Already has an explicit LIMIT — leave it alone.
        if _LIMIT_TAIL_RE.search(stripped):
            logger.debug("Query already contains a LIMIT clause; no change.")
            return query

        # Aggregation functions produce a single / small result set.
        if _AGGREGATION_RE.search(stripped):
            logger.debug(
                "Query uses an aggregation function; LIMIT not appended."
            )
            return query

        # Append a safety limit.
        limited = f"{stripped} LIMIT {default_limit}"
        logger.warning(
            "Query had no LIMIT clause — appended 'LIMIT %d'. "
            "Modified query: %s",
            default_limit,
            limited,
        )
        return limited

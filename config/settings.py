"""
settings.py – Neo4j Configuration
===================================
Dataclass-based configuration for the Neo4j database connection.

Values are read from a ``.env`` file (if present) and environment
variables, with safe fallback defaults that match
``config/settings.yaml`` for local development.

Environment Variables
---------------------
NEO4J_URI      : Bolt URI            (default: ``bolt://localhost:7687``)
NEO4J_USER     : Authentication user (default: ``neo4j``)
NEO4J_PASSWORD : Authentication pass (default: ``changeme``)
NEO4J_DATABASE : Target database     (default: ``neo4j``)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (one level up from config/)
_env_path: Path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)


@dataclass(frozen=True)
class Neo4jSettings:
    """Immutable container for Neo4j connection parameters.

    Attributes
    ----------
    uri : str
        Bolt protocol URI for the Neo4j instance.
    user : str
        Username for Neo4j authentication.
    password : str
        Password for Neo4j authentication.
    database : str
        Name of the target Neo4j database.
    """

    uri: str = field(
        default_factory=lambda: os.getenv("NEO4J_URI", "bolt://localhost:7687")
    )
    user: str = field(
        default_factory=lambda: os.getenv("NEO4J_USER", "neo4j")
    )
    password: str = field(
        default_factory=lambda: os.getenv("NEO4J_PASSWORD", "changeme")
    )
    database: str = field(
        default_factory=lambda: os.getenv("NEO4J_DATABASE", "neo4j")
    )

    def __repr__(self) -> str:  # noqa: D105
        """Mask the password in repr output to prevent accidental leakage."""
        return (
            f"Neo4jSettings(uri={self.uri!r}, user={self.user!r}, "
            f"password='***', database={self.database!r})"
        )


# Module-level singleton – import this where a config instance is needed.
neo4j_settings = Neo4jSettings()

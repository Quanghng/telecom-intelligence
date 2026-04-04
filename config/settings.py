"""
settings.py – Application Configuration
=========================================
Pydantic ``BaseSettings``-based configuration for the telecom-intelligence
GraphRAG pipeline.

Values are read from a ``.env`` file (if present) and environment
variables, with safe fallback defaults that match
``config/settings.yaml`` for local development.

Environment Variables
---------------------
NEO4J_URI      : Bolt URI            (default: ``bolt://localhost:7687``)
NEO4J_USER     : Authentication user (default: ``neo4j``)
NEO4J_PASSWORD : Authentication pass (default: ``changeme``)
NEO4J_DATABASE : Target database     (default: ``neo4j``)

LLM_API_KEY    : API key for the LLM provider (required)
LLM_MODEL      : Model identifier    (default: ``gpt-4o``)
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# .env lives at the project root (one level up from config/)
_env_path: Path = Path(__file__).resolve().parent.parent / ".env"


class Neo4jSettings(BaseSettings):
    """Neo4j connection parameters.

    Attributes
    ----------
    uri : str
        Bolt protocol URI for the Neo4j instance.
    user : str
        Username for Neo4j authentication.
    password : SecretStr
        Password for Neo4j authentication.
    database : str
        Name of the target Neo4j database.
    """

    model_config = SettingsConfigDict(
        env_file=str(_env_path),
        env_prefix="NEO4J_",
        extra="ignore",
    )

    uri: str = Field(default="bolt://localhost:7687")
    user: str = Field(default="neo4j")
    password: SecretStr = Field(default=SecretStr("changeme"))
    database: str = Field(default="neo4j")

    def __repr__(self) -> str:  # noqa: D105
        """Mask the password in repr output to prevent accidental leakage."""
        return (
            f"Neo4jSettings(uri={self.uri!r}, user={self.user!r}, "
            f"password='***', database={self.database!r})"
        )


class LLMSettings(BaseSettings):
    """LLM / AI provider configuration (OpenAI).

    Attributes
    ----------
    api_key : SecretStr
        API key for the OpenAI platform.
    model : str
        OpenAI model identifier to use for completions.
    """

    model_config = SettingsConfigDict(
        env_file=str(_env_path),
        env_prefix="LLM_",
        extra="ignore",
    )

    api_key: SecretStr = Field(default=...)
    model: str = Field(default="gpt-4o")


class AppSettings(BaseSettings):
    """Root configuration that aggregates all sub-settings."""

    model_config = SettingsConfigDict(
        env_file=str(_env_path),
        extra="ignore",
    )

    neo4j: Neo4jSettings = Neo4jSettings()
    llm: LLMSettings = LLMSettings()


# Module-level singletons – import these where a config instance is needed.
neo4j_settings = Neo4jSettings()
llm_settings = LLMSettings()
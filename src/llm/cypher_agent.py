"""
cypher_agent.py – CypherGenerationAgent
=========================================
Translates natural-language telecom questions into **read-only** Cypher
queries that are safe to execute against the telecom digital-twin
Neo4j graph.

The agent wraps a Hugging Face Serverless Inference API client
(via the ``openai`` SDK) and injects a strict system prompt that:

* enumerates every label, relationship type, and property in the graph
* forbids all write/mutation keywords
* constrains output to a single raw Cypher string

Usage
-----
    >>> from openai import OpenAI
    >>> from src.llm.cypher_agent import CypherGenerationAgent
    >>> client = OpenAI(base_url="https://api-inference.huggingface.co/v1", api_key="hf_...")
    >>> agent = CypherGenerationAgent(client)
    >>> cypher = agent.generate_query("Show me all down routers")
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from openai import OpenAI

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# System prompt – the single source of schema truth for the LLM
# ------------------------------------------------------------------

_SYSTEM_PROMPT: str = """\
You are a Cypher query generator for a telecom digital-twin knowledge graph \
stored in Neo4j. Your ONLY job is to convert the user's natural-language \
question into a single, syntactically correct Cypher query and return \
NOTHING ELSE — no explanation, no markdown fences, no commentary.

## GRAPH SCHEMA (follow EXACTLY)

### Base label
Every node in the graph carries the label `TelecomNode`.
Always include `TelecomNode` when matching nodes.

### Layer labels (STRICTLY lowercase, applied as additional Neo4j labels)
  - `physical`
  - `logical`
  - `service`
  - `business`

### Node types (stored in the `node_type` property)
  Physical layer  : Core_Router, Edge_Router, Cell_Tower
  Logical layer   : VLAN, MPLS_Tunnel
  Service layer   : VoLTE, 5G_Slice, Enterprise_VPN, IPTV
  Business layer  : Customer

### Node properties (available on every TelecomNode)
  - node_id   (unique identifier, string)
  - node_type (one of the node types listed above)
  - name      (human-readable name, string)
  - status    (one of: "up", "Degraded", "Down")
  - layer     (one of: "physical", "logical", "service", "business")

### Layer-specific properties
  Physical edges : capacity_gbps (int), latency_ms (float)
  Business nodes : sla (one of: "Gold", "Silver", "Bronze")

### Relationship types (UPPERCASE, directed)
  - CONNECTS_TO   (physical ↔ physical)
  - RUNS_ON       (logical  → physical)
  - DEPENDS_ON    (service  → logical)
  - SUBSCRIBED_TO (business → service)

## STRICT RULES
1. ONLY emit READ-ONLY Cypher: MATCH, OPTIONAL MATCH, WHERE, WITH, RETURN, ORDER BY, LIMIT, UNION, CASE.
2. NEVER use CREATE, MERGE, DELETE, SET, or DROP.
3. DO NOT use parameterized queries (e.g., NEVER use `$status`). Inline all concrete values.
4. CRITICAL SYNTAX: NEVER put 'OR' inside curly braces. If you need to match multiple statuses, you MUST use a WHERE clause with the IN operator.
5. Always include a LIMIT clause (default 25).
6. Return only the raw Cypher string. No markdown, no backticks.

## EXAMPLES

User: "Find any physical routers that are currently 'Down' or 'Degraded' and the services that depend on them."
Cypher:
MATCH (r:TelecomNode:physical) 
WHERE r.status IN ['Down', 'Degraded']
OPTIONAL MATCH (r)-[:RUNS_ON]->(l:TelecomNode:logical)-[:DEPENDS_ON]->(s:TelecomNode:service)
RETURN r, l, s
LIMIT 25
"""

# Regex to strip markdown code fences the LLM sometimes adds despite
# the system prompt instructions.
_FENCE_RE: re.Pattern[str] = re.compile(
    r"^```(?:cypher|sql|plaintext)?\s*\n?|```\s*$",
    re.MULTILINE | re.IGNORECASE,
)


# ------------------------------------------------------------------
# Agent
# ------------------------------------------------------------------

class CypherGenerationAgent:
    """Converts natural-language telecom questions into Cypher queries.

    Parameters
    ----------
    client : openai.OpenAI
        An **already-initialised** OpenAI-compatible client pointed at
        the Hugging Face Serverless Inference API.
    model : str, optional
        Model to use (default ``"meta-llama/Meta-Llama-3-8B-Instruct"``).
    temperature : float, optional
        Sampling temperature.  Low values (0.0–0.2) are recommended
        for deterministic Cypher output.

    Examples
    --------
    >>> from openai import OpenAI
    >>> client = OpenAI(base_url="https://api-inference.huggingface.co/v1", api_key="hf_...")
    >>> agent = CypherGenerationAgent(client)
    >>> agent.generate_query("List all degraded cell towers")
    'MATCH (n:TelecomNode:physical) WHERE n.node_type = "Cell_Tower" ...'
    """

    def __init__(
        self,
        client: OpenAI,
        model: str = "meta-llama/Meta-Llama-3-8B-Instruct",
        temperature: float = 0.0,
    ) -> None:
        self._client: OpenAI = client
        self._model: str = model
        self._temperature: float = temperature
        logger.info(
            "CypherGenerationAgent initialised (model=%s, temperature=%.1f)",
            self._model,
            self._temperature,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_query(self, user_prompt: str) -> str:
        """Translate *user_prompt* into a read-only Cypher query.

        Parameters
        ----------
        user_prompt : str
            A natural-language question about the telecom graph
            (e.g. ``"Which Gold-SLA customers depend on degraded services?"``).

        Returns
        -------
        str
            A raw Cypher query string ready to be passed through
            :class:`~src.database.sanitizer.QuerySanitizer` and then
            executed via
            :meth:`~src.database.connection.Neo4jConnection.execute_llm_read_query`.

        Raises
        ------
        openai.APIError
            Propagated from the underlying SDK on API failure.
        ValueError
            If the LLM returns an empty response.
        """
        logger.info("Generating Cypher for user prompt: %s", user_prompt)

        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=self._temperature,
        )

        raw: Optional[str] = response.choices[0].message.content
        if not raw or not raw.strip():
            logger.error("LLM returned an empty response for prompt: %s", user_prompt)
            raise ValueError(
                "LLM returned an empty Cypher response. "
                "Try rephrasing the question."
            )

        cypher: str = self._clean_response(raw)
        logger.info("Generated Cypher:\n  %s", cypher)
        return cypher

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_response(raw: str) -> str:
        """Strip markdown fences and surrounding whitespace.

        Despite explicit instructions, LLMs occasionally wrap the
        Cypher in triple-backtick fences.  This method removes them
        so downstream consumers receive a clean query string.
        """
        cleaned: str = _FENCE_RE.sub("", raw).strip()
        # Remove a trailing semicolon (Neo4j driver doesn't want it).
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1].rstrip()
        return cleaned

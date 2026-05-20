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
    >>> client = OpenAI(base_url="https://router.huggingface.co/v1", api_key="hf_...")
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
You are a Cypher query generator for a telecom digital-twin. 
Your ONLY job is to return a single, raw, read-only Cypher query. No explanations.

## GRAPH SCHEMA
Labels: TelecomNode, physical, logical, service, business
Properties: node_id, node_type, name, status, layer, cpu_utilization, sla
Relationships: 
  (logical)-[:RUNS_ON]->(physical)
  (service)-[:DEPENDS_ON]->(logical)
  (business)-[:SUBSCRIBED_TO]->(service)
  (physical)-[:CONNECTS_TO]-(physical)

## CRITICAL ONTOLOGY RULES — NEVER VIOLATE THESE
You MUST strictly adhere to these directed relationships. NEVER reverse them.
1. (logical)-[:RUNS_ON]->(physical)     — Logical nodes run ON physical nodes. Arrow points TO physical.
2. (service)-[:DEPENDS_ON]->(logical)   — Service nodes depend ON logical nodes. Arrow points TO logical.
3. (business)-[:SUBSCRIBED_TO]->(service) — Business nodes subscribe TO service nodes. Arrow points TO service.
4. (physical)-[:CONNECTS_TO]-(physical) — This is BIDIRECTIONAL (no arrow direction).

When traversing FROM physical UP TO business, REVERSE the arrows:
  (physical)<-[:RUNS_ON]-(logical)<-[:DEPENDS_ON]-(service)<-[:SUBSCRIBED_TO]-(business)

When traversing FROM business DOWN TO physical, FOLLOW the arrows:
  (business)-[:SUBSCRIBED_TO]->(service)-[:DEPENDS_ON]->(logical)-[:RUNS_ON]->(physical)

## STRICT RULES
1. node_type is a PROPERTY, NEVER a label. Correct: `(n:TelecomNode:physical {node_type: 'Core_Router'})`
2. Numeric conversion: `WHERE toFloat(split(n.cpu_utilization, '%')[0]) > 95`
3. NEVER use shortestPath(), *, or variable length paths like [:REL*] or [:REL*1..2]. Always use exactly one relationship per hop.
4. ALWAYS use the exact relationship arrows shown in the CRITICAL ONTOLOGY RULES.
5. If the user mentions a specific node name, use it. But NEVER filter on both `name` AND `node_type` AND `status` simultaneously in inline properties — put extra filters in the WHERE clause so partial matches still return results.
6. For multi-hop traversals (RUNS_ON → DEPENDS_ON → SUBSCRIBED_TO), always chain single-hop patterns. NEVER use variable-length syntax.
7. ALWAYS include LIMIT 25 at the end.
8. When returning nodes, ALWAYS return the full node object (e.g. `RETURN n, m`) instead of individual properties (e.g. `RETURN n.name, n.node_id`). This ensures the downstream Synthesis Agent receives all properties including node_type, layer, status, and labels. For relationships, return the full relationship object too (e.g. `RETURN n, r, m`).

## FORBIDDEN PATTERNS (NEVER GENERATE THESE)
✗ MATCH (a)-[:RUNS_ON*]->(b)          — variable-length path
✗ MATCH (a)-[:CONNECTS_TO*1..2]-(b)   — bounded variable-length path  
✗ MATCH p = shortestPath((a)-[*]-(b)) — shortestPath
✗ RETURN n.name, n.node_id, n.status  — partial property selection (loses context)
✗ (physical)-[:RUNS_ON]->(logical)     — WRONG direction! Logical runs on physical, not the reverse.
✗ (logical)-[:DEPENDS_ON]->(service)   — WRONG direction! Service depends on logical, not the reverse.
✗ (service)-[:SUBSCRIBED_TO]->(business) — WRONG direction! Business subscribes to service, not the reverse.

✓ CORRECT multi-hop pattern (physical UP TO business):
MATCH (ct:TelecomNode:physical)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business)
WHERE ct.node_type = 'Cell_Tower' AND ct.status = 'Degraded'
RETURN ct, l, s, c
LIMIT 25

## EXACT EXAMPLES (PATTERN MATCH THESE STRUCTURES)

Question: Which physical-layer TelecomNodes with node_type 'Core_Router' currently have status 'Down'?
Cypher:
MATCH (n:TelecomNode:physical {node_type: 'Core_Router'}) 
WHERE n.status = 'Down' 
RETURN n 
LIMIT 25

Question: Which logical-layer VirtualRouters run on a physical-layer Edge_Router that currently has status 'Degraded'?
Cypher:
MATCH (l:TelecomNode:logical {node_type: 'VirtualRouter'})-[:RUNS_ON]->(p:TelecomNode:physical {node_type: 'Edge_Router'}) 
WHERE p.status = 'Degraded' 
RETURN l, p 
LIMIT 25

Question: A logical-layer Subnet has status 'Degraded'; which physical-layer Switch does it run on?
Cypher:
MATCH (v:TelecomNode:logical {node_type: 'Subnet'})-[:RUNS_ON]->(r:TelecomNode:physical {node_type: 'Switch'}) 
WHERE v.status = 'Degraded' 
RETURN v, r 
LIMIT 25

Question: If a physical-layer Core_Router with status 'Down' has logical-layer nodes running on it, which service-layer nodes depend on those logical nodes?
Cypher:
MATCH (p:TelecomNode:physical {node_type: 'Core_Router'})<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service) 
WHERE p.status = 'Down' 
RETURN p, l, s 
LIMIT 25

Question: If physical-layer Edge_Router 'XYZ' goes 'Down', which business-layer customers are affected?
Cypher:
MATCH (r:TelecomNode:physical {node_type: 'Edge_Router', name: 'XYZ'}) 
WHERE r.status = 'Down' 
OPTIONAL MATCH (r)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business) 
RETURN r, l, s, c 
LIMIT 25

Question: Is there a path from a physical-layer Core_Router with status 'Down' to another physical-layer Edge_Router via CONNECTS_TO, and do any business-layer customers depend on that Edge_Router?
Cypher:
MATCH (cr:TelecomNode:physical {node_type: 'Core_Router'})-[:CONNECTS_TO]-(er:TelecomNode:physical {node_type: 'Edge_Router'}) 
WHERE cr.status = 'Down' 
OPTIONAL MATCH (er)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business) 
RETURN cr, er, l, s, c 
LIMIT 25

Question: For each CONNECTS_TO link with high latency, which business-layer Silver customers are reachable?
Cypher:
MATCH (a:TelecomNode:physical)-[link:CONNECTS_TO]-(b:TelecomNode:physical)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business {sla: 'Silver'}) 
WHERE toFloat(split(link.latency_ms, 'ms')[0]) > 50 
RETURN a, link, b, l, s, c 
LIMIT 25

Question: Which logical-layer VLANs with status 'Degraded' run on physical-layer nodes?
Cypher:
MATCH (v:TelecomNode:logical {node_type: 'VLAN'})-[:RUNS_ON]->(r:TelecomNode:physical)
WHERE v.status = 'Degraded'
RETURN v, r
LIMIT 25
"""

# Regex to strip markdown code fences the LLM sometimes adds despite
# the system prompt instructions.
_FENCE_RE: re.Pattern[str] = re.compile(
    r"^```(?:cypher|sql|plaintext)?\s*\n?|```\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Regex to detect variable-length relationship patterns
_VAR_LENGTH_RE: re.Pattern[str] = re.compile(
    r"\[:\w+\*[\d.]*\]|\[\*\]|shortestPath\s*\(",
    re.IGNORECASE,
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
        Model to use (default ``"Qwen/Qwen2.5-7B-Instruct"``).
    temperature : float, optional
        Sampling temperature.  Low values (0.0–0.2) are recommended
        for deterministic Cypher output.

    Examples
    --------
    >>> from openai import OpenAI
    >>> client = OpenAI(base_url="https://router.huggingface.co/v1", api_key="hf_...")
    >>> agent = CypherGenerationAgent(client)
    >>> agent.generate_query("List all degraded cell towers")
    'MATCH (n:TelecomNode:physical) WHERE n.node_type = "Cell_Tower" ...'
    """

    def __init__(
        self,
        client: OpenAI,
        model: str = "Qwen/Qwen2.5-7B-Instruct",
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
        """Translate *user_prompt* into a read-only Cypher query."""
        logger.info("Generating Cypher for user prompt: %s", user_prompt)
        cypher = self._call_llm(user_prompt)
        return cypher

    def generate_query_with_fallback(
        self, user_prompt: str, graph_data_length: int
    ) -> str | None:
        """Re-generate a broader Cypher if the first attempt returned 0 rows.

        Parameters
        ----------
        user_prompt : str
            The original natural-language question.
        graph_data_length : int
            Number of rows returned by the first Cypher execution.

        Returns
        -------
        str | None
            A relaxed Cypher query, or ``None`` if no fallback is needed.
        """
        if graph_data_length > 0:
            return None

        relaxed_prompt = (
            f"The previous Cypher query for the following question returned 0 results. "
            f"Generate a BROADER query that removes specific name filters or status "
            f"filters to return partial matches instead. Question: {user_prompt}"
        )
        logger.warning(
            "Zero results — generating fallback Cypher for: %s", user_prompt
        )
        return self._call_llm(relaxed_prompt)

    def _call_llm(self, user_prompt: str) -> str:
        """Low-level LLM call shared by generate_query and fallback."""
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
        """Strip markdown fences and surrounding whitespace."""
        cleaned: str = _FENCE_RE.sub("", raw).strip()
        # Remove a trailing semicolon (Neo4j driver doesn't want it).
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1].rstrip()

        # Reject queries with variable-length paths
        if _VAR_LENGTH_RE.search(cleaned):
            logger.error(
                "Generated Cypher contains forbidden variable-length path: %s",
                cleaned,
            )
            raise ValueError(
                "LLM generated a variable-length path pattern which is "
                "forbidden by the schema constraints. The query has been "
                "rejected to prevent Cartesian explosions."
            )

        return cleaned

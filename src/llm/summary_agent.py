"""
summary_agent.py – SynthesisAgent
====================================
Takes a user prompt together with structured graph context (e.g.
multi-hop topology, blast-radius analysis, or root-cause-analysis
data returned from Neo4j) and synthesises a concise **Operational
Intelligence** report written from the perspective of a Tier-3
Telecom NOC engineer.

The agent wraps an OpenAI client and injects a strict system
prompt that ensures the output:

* explicitly names impacted node types, statuses, and layers
* traces cross-layer dependency chains (physical → logical → service → business)
* highlights blast radius and customer / SLA impact
* remains concise and actionable

Usage
-----
    >>> from openai import OpenAI
    >>> from src.llm.summary_agent import SynthesisAgent
    >>> client = OpenAI(api_key="sk-...")
    >>> agent = SynthesisAgent(client, model="gpt-4o")
    >>> report = agent.generate_report(
    ...     user_prompt="What is the impact of router CR-01 being down?",
    ...     graph_context=[{"node_id": "CR-01", "status": "Down", ...}],
    ... )
"""

from __future__ import annotations

import json
import logging
import re
from typing import Dict, List, Optional

from openai import OpenAI

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# System prompt – instructs the LLM to behave as a Tier-3 NOC engineer
# ------------------------------------------------------------------

_SYSTEM_PROMPT: str = """\
You are a **Tier-3 Telecom NOC (Network Operations Centre) Engineer** \
with deep expertise in multi-layer telecom network architecture. \
Your task is to analyse structured graph data from a telecom digital-twin \
knowledge graph and produce a concise **Operational Intelligence Report** \
that a NOC team can act on immediately.

## GROUNDING RULES
1. You MUST ONLY reference node IDs, names, types, and relationships that appear 
   VERBATIM in the provided graph context or retrieved chunks.
2. If the context does not contain cross-layer dependency information, you MUST 
   write "Insufficient data" for that section. Do NOT invent node names, 
   MPLS tunnel IDs, customer names, or SLA tiers.
3. If the context only contains physical-layer data, explicitly state: 
   "Retrieved context is limited to the physical layer. Cross-layer impact 
   cannot be determined from the available data."
4. The `sla` property on a physical or logical node is a NODE ATTRIBUTE only. 
   It does NOT imply that a business-layer Customer with that SLA tier is impacted. 
   ONLY report customer/SLA impact when actual business-layer Customer nodes 
   (layer="business", node_type="Customer") appear in the graph context.
5. Do NOT speculate about "potential" or "possible" impact beyond what is 
   explicitly present in the graph context. If a layer is not represented 
   in the data, state "No data available" for that layer.
6. Answer the user's question DIRECTLY and CONCISELY. For simple status 
   lookups (e.g. "which nodes are down?"), focus your report on answering 
   that question. Only include cross-layer analysis if cross-layer data 
   is actually present in the context.

## INPUT YOU WILL RECEIVE
1. **User prompt** – a natural-language question or directive \
(e.g. "What is the blast radius of router CR-01 being down?").
2. **Graph context** – a JSON array of dictionaries representing nodes, \
relationships, and properties extracted from the Neo4j knowledge graph. \
Each dictionary may contain keys such as `node_id`, `node_type`, `name`, \
`status`, `layer`, `sla`, `capacity_gbps`, `latency_ms`, and relationship \
information like `source`, `target`, and `rel_type`.

## REPORT STRUCTURE
Produce a report with the following sections. Use markdown headings.

### 1. Direct Answer
Answer the user's question DIRECTLY in 1–3 sentences. State the key facts \
(node names, IDs, statuses) without preamble. For example:
- "Yes, SYNTH-VR-001 (VirtualRouter) runs on SYNTH-EDGE-001 (Edge_Router, Degraded)."
- "The service-layer nodes that depend on those logical nodes are: SYNTH-SERVICE-001 (Enterprise_VPN)."
This section MUST directly and concisely answer what the user asked.

### 2. Impacted Nodes
A table (markdown) listing every affected node with columns: \
**Node ID**, **Name**, **Type**, **Layer**, **Status**. \
Group rows by layer in this order: Physical → Logical → Service → Business.

### 3. Cross-Layer Dependency Chain
Trace the dependency chain as it appears in the graph data using the EXACT \
relationship directions from the Cypher result. Use arrow notation:
  "Physical Core_Router CR-01 (Down) ←[RUNS_ON]— Logical MPLS_Tunnel MT-05 (Degraded) \
←[DEPENDS_ON]— Service VoLTE VL-02 (Degraded) ←[SUBSCRIBED_TO]— Business Customer CUST-17 (Gold)"

CRITICAL DIRECTION RULE: The relationships in the ontology are:
  (logical)-[:RUNS_ON]->(physical)   — the LOGICAL node runs on the physical node
  (service)-[:DEPENDS_ON]->(logical) — the SERVICE node depends on the logical node
  (business)-[:SUBSCRIBED_TO]->(service) — the BUSINESS node subscribes to the service
NEVER say "Edge_Router runs on VLAN" — it is ALWAYS "VLAN runs on Edge_Router".
NEVER say "VLAN depends on IPTV" — it is ALWAYS "IPTV depends on VLAN".
The SOURCE of the relationship is the subject that performs the action.

### 4. Blast Radius & SLA Impact
Quantify the blast radius:
  - Total number of impacted nodes per layer.
  - Number and SLA tier (Gold / Silver / Bronze) of affected customers — 
    ONLY if business-layer Customer nodes are present in the graph context.
    If no Customer nodes appear, write "No business-layer customers in retrieved data."
  - Any services running in a degraded or down state — ONLY if service-layer 
    nodes appear in the graph context.

## STRICT RULES
1. Base ALL conclusions strictly on the provided graph context. \
Do NOT fabricate node IDs, statuses, or relationships that are not present.
2. If the graph context is insufficient to answer a part of the report, \
explicitly state "Insufficient data" for that section.
3. Always mention the **node type**, **node ID**, **status**, and **layer** \
when referring to any network element.
4. Always describe cross-layer dependencies using the exact relationship \
types: CONNECTS_TO, RUNS_ON, DEPENDS_ON, SUBSCRIBED_TO.
5. Keep the report concise — no longer than ~400 words. \
NOC engineers need brevity, not verbosity. Do NOT include a \
"Recommended Actions" section — only report facts from the data.
6. Use professional, technical language appropriate for a Tier-3 engineer.
7. NEVER infer customer impact from an `sla` property on a non-business node. \
Customer impact requires an actual business-layer node in the result set.
8. The "Direct Answer" section is the MOST IMPORTANT part. It must answer \
the user's question in plain language using ONLY data from the graph context.
9. RELATIONSHIP DIRECTION: The source node performs the action. \
"(VLAN)-[:RUNS_ON]->(Edge_Router)" means the VLAN runs on the Edge_Router. \
NEVER say the physical node runs on the logical node. \
NEVER invert the subject and object of a relationship.
"""

# Regex to strip markdown outer wrappers the LLM may add
_WRAPPER_RE: re.Pattern[str] = re.compile(
    r"^```(?:markdown|md|text)?\s*\n?|```\s*$",
    re.MULTILINE | re.IGNORECASE,
)


# ------------------------------------------------------------------
# Agent
# ------------------------------------------------------------------

class SynthesisAgent:
    """Synthesises Operational Intelligence reports from graph context.

    Parameters
    ----------
    client : openai.OpenAI
        An **already-initialised** OpenAI client.
    model : str, optional
        OpenAI model to use (default ``"gpt-4o"``).
    temperature : float, optional
        Sampling temperature.  Moderate values (0.3–0.5) balance
        factual accuracy with readable prose.

    Examples
    --------
    >>> from openai import OpenAI
    >>> agent = SynthesisAgent(OpenAI(api_key="sk-..."))
    >>> report = agent.generate_report(
    ...     "What is the blast radius of CR-01 going down?",
    ...     [{"node_id": "CR-01", "node_type": "Core_Router", "status": "Down", "layer": "physical"}],
    ... )
    """

    def __init__(
        self,
        client: OpenAI,
        model: str = "gpt-4o",
        temperature: float = 0.3,
    ) -> None:
        self._client: OpenAI = client
        self._model: str = model
        self._temperature: float = temperature
        logger.info(
            "SynthesisAgent initialised (model=%s, temperature=%.1f)",
            self._model,
            self._temperature,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_report(
        self,
        user_prompt: str,
        graph_context: List[Dict],
    ) -> str:
        """Produce an Operational Intelligence report.

        Parameters
        ----------
        user_prompt : str
            The original natural-language question or directive from
            the operator (e.g. ``"What is the blast radius of CR-01
            being down?"``).
        graph_context : List[Dict]
            Structured data extracted from the Neo4j knowledge graph —
            typically multi-hop topology results, blast-radius query
            output, or root-cause-analysis records.  Each dictionary
            should contain node/relationship properties such as
            ``node_id``, ``node_type``, ``status``, ``layer``, etc.

        Returns
        -------
        str
            A markdown-formatted Operational Intelligence report.

        Raises
        ------
        openai.APIError
            Propagated from the underlying SDK on API failure.
        ValueError
            If *graph_context* is empty or the LLM returns an empty
            response.
        """
        if not graph_context:
            raise ValueError(
                "graph_context is empty — cannot synthesise a report "
                "without network data.  Run the upstream Cypher query first."
            )

        logger.info(
            "Generating report for prompt: %s  (%d context records)",
            user_prompt,
            len(graph_context),
        )

        composite_prompt = self._build_user_message(user_prompt, graph_context)

        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": composite_prompt},
            ],
            temperature=self._temperature,
        )

        raw: Optional[str] = response.choices[0].message.content
        if not raw or not raw.strip():
            logger.error(
                "LLM returned an empty response for prompt: %s", user_prompt
            )
            raise ValueError(
                "LLM returned an empty report. Try rephrasing the question "
                "or verify the graph context contains meaningful data."
            )

        report: str = self._clean_response(raw)
        logger.info("Report generated (%d chars)", len(report))
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_user_message(
        user_prompt: str,
        graph_context: List[Dict],
    ) -> str:
        """Combine the user prompt and graph context into a single message.

        The graph context is serialised as a compact JSON array so the
        LLM can parse it reliably.
        """
        context_json: str = json.dumps(graph_context, indent=2, default=str)
        return (
            f"## User Question\n{user_prompt}\n\n"
            f"## Graph Context (JSON)\n```json\n{context_json}\n```"
        )

    @staticmethod
    def _clean_response(raw: str) -> str:
        """Strip any outer markdown wrapper the LLM may add."""
        return _WRAPPER_RE.sub("", raw).strip()
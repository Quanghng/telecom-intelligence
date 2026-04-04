"""
orchestrator.py – GraphRAGOrchestrator
=======================================
End-to-end coordinator for the telecom digital-twin GraphRAG pipeline.

The orchestrator wires three components into a single
``run_pipeline`` call:

1. **CypherGenerationAgent** – translates a natural-language question
   into a read-only Cypher query.
2. **Neo4jConnection.execute_llm_read_query** – safely executes that
   Cypher against the knowledge graph and returns structured records.
3. **SynthesisAgent** – consumes the graph data together with the
   original question and produces a concise Operational Intelligence
   report.

Usage
-----
    >>> from openai import OpenAI
    >>> from src.database.connection import Neo4jConnection
    >>> from src.llm import CypherGenerationAgent, SynthesisAgent
    >>> from src.core.orchestrator import GraphRAGOrchestrator
    >>>
    >>> client = OpenAI(api_key="sk-...")
    >>> cypher_agent = CypherGenerationAgent(client)
    >>> synthesis_agent = SynthesisAgent(client)
    >>> with Neo4jConnection() as conn:
    ...     orch = GraphRAGOrchestrator(conn, cypher_agent, synthesis_agent)
    ...     result = orch.run_pipeline("What is the blast radius of CR-01 going down?")
    ...     print(result["final_report"])
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.database.connection import Neo4jConnection
from src.llm.cypher_agent import CypherGenerationAgent
from src.llm.summary_agent import SynthesisAgent

logger: logging.Logger = logging.getLogger(__name__)


class GraphRAGOrchestrator:
    """Coordinates the full GraphRAG pipeline: NL → Cypher → Graph → Report.

    Parameters
    ----------
    connection : Neo4jConnection
        An **already-initialised** (i.e. entered via ``with``) database
        connection that exposes ``execute_llm_read_query``.
    cypher_agent : CypherGenerationAgent
        Agent that translates natural-language prompts into Cypher.
    synthesis_agent : SynthesisAgent
        Agent that synthesises an Operational Intelligence report from
        graph data.

    Examples
    --------
    >>> with Neo4jConnection() as conn:
    ...     orch = GraphRAGOrchestrator(conn, cypher_agent, synthesis_agent)
    ...     result = orch.run_pipeline("Show all down routers and their impact")
    ...     print(result["generated_cypher"])
    ...     print(result["final_report"])
    """

    def __init__(
        self,
        connection: Neo4jConnection,
        cypher_agent: CypherGenerationAgent,
        synthesis_agent: SynthesisAgent,
    ) -> None:
        self._connection: Neo4jConnection = connection
        self._cypher_agent: CypherGenerationAgent = cypher_agent
        self._synthesis_agent: SynthesisAgent = synthesis_agent
        logger.info("GraphRAGOrchestrator initialised.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_pipeline(self, user_query: str) -> Dict[str, Any]:
        """Execute the full GraphRAG pipeline for *user_query*.

        Workflow
        --------
        1. Pass *user_query* to the Cypher agent → ``cypher_str``.
        2. Execute ``cypher_str`` via the database connection's
           ``execute_llm_read_query`` → ``results`` (list of dicts).
        3. Pass *user_query* + ``results`` to the Synthesis agent
           → ``report`` (markdown string).

        Parameters
        ----------
        user_query : str
            A natural-language question about the telecom knowledge
            graph (e.g. ``"What is the blast radius of router CR-01
            being down?"``).

        Returns
        -------
        Dict[str, Any]
            A dictionary with the following keys:

            * ``user_query``        – the original question.
            * ``generated_cypher``  – the Cypher string produced by
              the LLM.
            * ``graph_data_length`` – number of records returned by
              Neo4j.
            * ``final_report``      – the synthesised Operational
              Intelligence report (markdown).

        Raises
        ------
        ValueError
            If the Cypher agent or Synthesis agent returns an empty
            response, or the graph query yields no data.
        SecurityViolationError
            If the generated Cypher contains write/mutation keywords.
        neo4j.exceptions.Neo4jError
            On any driver- or server-side execution error.
        """
        logger.info("Pipeline started for query: %s", user_query)

        # Step 1 – Natural language → Cypher
        logger.info("Step 1/3: Generating Cypher query …")
        cypher_str: str = self._cypher_agent.generate_query(user_query)
        logger.info("Cypher generated: %s", cypher_str)

        # Step 2 – Cypher → Graph data (sanitised read-only execution)
        logger.info("Step 2/3: Executing Cypher against Neo4j …")
        results: List[Dict[str, Any]] = self._connection.execute_llm_read_query(
            cypher_str,
        )
        logger.info("Neo4j returned %d record(s).", len(results))

        # Step 3 – Graph data + original query → Operational report
        logger.info("Step 3/3: Synthesising Operational Intelligence report …")
        report: str = self._synthesis_agent.generate_report(
            user_prompt=user_query,
            graph_context=results,
        )
        logger.info("Report synthesis complete (%d chars).", len(report))

        payload: Dict[str, Any] = {
            "user_query": user_query,
            "generated_cypher": cypher_str,
            "graph_data_length": len(results),
            "final_report": report,
        }

        logger.info("Pipeline finished successfully.")
        return payload
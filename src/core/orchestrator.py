"""
orchestrator.py – GraphRAGOrchestrator (LangGraph StateGraph)
==============================================================
End-to-end coordinator for the telecom digital-twin GraphRAG pipeline,
implemented as a **cyclical Directed State Graph** using LangGraph.

Architecture
------------
Unlike a fragile linear pipeline (generate → execute → synthesise),
this orchestrator models the full pipeline as a state machine with a
**feedback loop** that catches Neo4j syntax errors and empty result
sets, feeding error context back into the LLM for self-correction.

.. code-block:: text

    ┌─────────────────────────────────────────────────────────────────┐
    │                         START                                    │
    └──────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │  Node 1: RETRIEVE CONTEXT                                       │
    │  • Embed user query via text-embedding-3-small                  │
    │  • Query ChromaDB for k=3 most similar Cypher examples          │
    │  • Compute schema union (Dynamic Context Assembly)               │
    └──────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │  Node 2: GENERATE CYPHER                                        │
    │  • Assemble dynamic prompt (schema union + few-shot + errors)    │
    │  • Call HuggingFace LLM via OpenAI SDK                          │
    │  • Strip markdown fences, increment iteration_count              │
    └──────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │  Node 3: EXECUTE NEO4J                                          │
    │  • Run Cypher via Neo4jConnection.execute_llm_read_query()      │
    │  • On syntax error → capture exception into error_feedback       │
    │  • On 0 results   → set error_feedback with broadening hint     │
    │  • On success     → populate graph_data, clear error_feedback    │
    └───────────┬─────────────────────────────────┬───────────────────┘
                │ SUCCESS                         │ ERROR
                ▼                                 ▼
    ┌───────────────────────┐     ┌───────────────────────────────────┐
    │  Node 4: SYNTHESIZE   │     │  Conditional Edge:                │
    │  • Generate report    │     │  if iteration < 3 → Node 2       │
    │  • Return final state │     │  else → Node 4 (graceful fail)   │
    └───────────────────────┘     └───────────────────────────────────┘

State Definition
----------------
The graph state is a ``TypedDict`` (``GraphState``) containing all
data flowing through the pipeline.  Each node reads from and writes to
this shared state, enabling the cyclical feedback loop.

Usage
-----
    >>> from openai import OpenAI
    >>> from src.database.connection import Neo4jConnection
    >>> from src.llm.vector_memory import CypherVectorMemory
    >>> from src.core.orchestrator import GraphRAGOrchestrator
    >>>
    >>> hf_client = OpenAI(base_url="https://router.huggingface.co/v1", api_key="hf_...")
    >>> with Neo4jConnection() as conn:
    ...     orch = GraphRAGOrchestrator(
    ...         connection=conn,
    ...         llm_client=hf_client,
    ...         openai_api_key="sk-...",
    ...     )
    ...     result = orch.run_pipeline("What is the blast radius of CR-01 going down?")
    ...     print(result["final_report"])
"""

from __future__ import annotations

import logging
import re
import traceback
from typing import Any, Dict, List, Optional

from langgraph.graph import END, StateGraph
from openai import OpenAI
from typing_extensions import TypedDict

from src.database.connection import Neo4jConnection
from src.llm.summary_agent import SynthesisAgent
from src.llm.vector_memory import CypherVectorMemory, RetrievedContext

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

MAX_ITERATIONS: int = 3
"""Maximum generation→execution cycles before graceful failure."""

# Regex to strip markdown code fences the LLM sometimes adds
_FENCE_RE: re.Pattern[str] = re.compile(
    r"^```(?:cypher|sql|plaintext)?\s*\n?|```\s*$",
    re.MULTILINE | re.IGNORECASE,
)


# ------------------------------------------------------------------
# Graph State Definition (TypedDict)
# ------------------------------------------------------------------


class GraphState(TypedDict):
    """Typed state container for the full GraphRAG LangGraph pipeline.

    This TypedDict defines the complete state flowing through the
    directed state graph.  Each node reads from and writes to this
    shared state, enabling the Neo4j execution→regeneration feedback
    loop that is central to the Dynamic GraphRAG architecture.

    Attributes
    ----------
    user_query : str
        The original natural-language telecom question.
    chat_history : List[Dict[str, str]]
        Conversation history for multi-turn context (reserved for
        future extension).
    retrieved_schema : Dict[str, Any]
        The dynamically assembled schema union computed from the
        retrieved ChromaDB examples (Pillar 2: Dynamic Context
        Assembly).
    few_shot_examples : List[Dict[str, Any]]
        The top-k few-shot examples retrieved from the Cypher Query
        Library via ChromaDB (Pillar 1: Dynamic Vector Routing).
    generated_cypher : str
        The current Cypher query candidate produced by the LLM.
    graph_data : List[Dict[str, Any]]
        Records returned by Neo4j after successful Cypher execution.
    error_feedback : str
        Holds Neo4j syntax exception messages or empty-result
        warnings.  Fed back into the generation prompt on retry
        iterations.
    iteration_count : int
        Number of Cypher generation attempts (starts at 0).  Used
        by the conditional edge to enforce the MAX_ITERATIONS bound.
    final_report : str
        The synthesised Operational Intelligence report (markdown).
    """

    user_query: str
    chat_history: List[Dict[str, str]]
    retrieved_schema: Dict[str, Any]
    few_shot_examples: List[Dict[str, Any]]
    generated_cypher: str
    graph_data: List[Dict[str, Any]]
    error_feedback: str
    iteration_count: int
    final_report: str


# ------------------------------------------------------------------
# Dynamic System Prompt Template
# ------------------------------------------------------------------

_BASE_SYSTEM_PROMPT: str = """\
You are a Cypher query generator for a telecom digital-twin.
Your ONLY job is to return a single, raw, read-only Cypher query. No explanations.

## GRAPH SCHEMA (FULL ONTOLOGY)
Labels: TelecomNode, physical, logical, service, business
Properties: node_id, node_type, name, status, layer, cpu_utilization, sla
Relationships:
  (logical)-[:RUNS_ON]->(physical)
  (service)-[:DEPENDS_ON]->(logical)
  (business)-[:SUBSCRIBED_TO]->(service)
  (physical)-[:CONNECTS_TO]-(physical)

## VALID node_type VALUES — USE ONLY THESE, NEVER INVENT OTHERS
  Physical layer : Core_Router | Edge_Router | Cell_Tower
  Logical layer  : VLAN | MPLS_Tunnel | VirtualRouter
  Service layer  : IPTV | Enterprise_VPN | 5G_Slice | VoLTE
  Business layer : Customer

## VALID status VALUES
  up | Down | Degraded

## ROUTER TERMINOLOGY RULE
When the user says "router" or "routers" (without specifying Core or Edge),
ALWAYS match BOTH types: `WHERE n.node_type IN ['Core_Router', 'Edge_Router']`
NEVER use node_type = 'Router' — that value does not exist in the graph.

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

{dynamic_schema_block}

## STRICT RULES
1. node_type is a PROPERTY, NEVER a label. Correct: `(n:TelecomNode:physical {{node_type: 'Core_Router'}})`
2. Numeric conversion: `WHERE toFloat(split(n.cpu_utilization, '%')[0]) > 95`
3. NEVER use shortestPath(), *, or variable length paths like [:REL*] or [:REL*1..2]. Always use exactly one relationship per hop.
4. ALWAYS use the exact relationship arrows shown in the CRITICAL ONTOLOGY RULES.
5. If the user mentions a specific node name, use it. But NEVER filter on both `name` AND `node_type` AND `status` simultaneously in inline properties — put extra filters in the WHERE clause so partial matches still return results.
6. For multi-hop traversals (RUNS_ON → DEPENDS_ON → SUBSCRIBED_TO), always chain single-hop patterns. NEVER use variable-length syntax.
7. ALWAYS include LIMIT 25 at the end.
8. When returning nodes, ALWAYS return the full node object (e.g. `RETURN n, m`) instead of individual properties. For relationships, return the full relationship object too (e.g. `RETURN n, r, m`).

## FORBIDDEN PATTERNS (NEVER GENERATE THESE)
✗ MATCH (a)-[:RUNS_ON*]->(b)          — variable-length path
✗ MATCH (a)-[:CONNECTS_TO*1..2]-(b)   — bounded variable-length path
✗ MATCH p = shortestPath((a)-[*]-(b)) — shortestPath
✗ RETURN n.name, n.node_id, n.status  — partial property selection (loses context)
✗ (physical)-[:RUNS_ON]->(logical)     — WRONG direction!
✗ (logical)-[:DEPENDS_ON]->(service)   — WRONG direction!
✗ (service)-[:SUBSCRIBED_TO]->(business) — WRONG direction!

{dynamic_examples_block}
"""

_ERROR_FEEDBACK_ADDENDUM: str = """\

## PREVIOUS CYPHER ATTEMPT FAILED — YOU MUST FIX THIS:
Error from Neo4j: {error}

Generate a CORRECTED Cypher query that resolves this error. \
Do NOT repeat the same mistake.
"""


# ------------------------------------------------------------------
# GraphRAGOrchestrator – LangGraph StateGraph
# ------------------------------------------------------------------


class GraphRAGOrchestrator:
    """Full-pipeline LangGraph orchestrator with Neo4j feedback loop.

    This class implements the complete Dynamic GraphRAG architecture
    as a cyclical directed state graph.  Unlike a linear pipeline,
    the state machine catches **database-level** errors (syntax
    exceptions, empty results) and routes them back into the LLM
    for self-correction — up to ``MAX_ITERATIONS`` times.

    The three architectural pillars are realised at the orchestrator
    level:

    1. **Dynamic Vector Routing** (Node 1) — ChromaDB retrieval of
       semantically relevant few-shot examples.
    2. **Dynamic Context Assembly** (Node 2) — Schema union injection
       into the generation prompt.
    3. **Cyclical State Machine** (Conditional Edge) — Neo4j error
       feedback loop bounded by MAX_ITERATIONS.

    Parameters
    ----------
    connection : Neo4jConnection
        An already-initialised (entered via ``with``) Neo4j connection.
    llm_client : OpenAI
        OpenAI-compatible client for LLM generation (HuggingFace
        Serverless Inference API).
    openai_api_key : str
        API key for OpenAI embeddings (text-embedding-3-small).
    synthesis_agent : SynthesisAgent | None, optional
        Agent for final report synthesis.  If None, one is created
        using ``llm_client``.
    vector_memory : CypherVectorMemory | None, optional
        Pre-initialised ChromaDB memory.  If None, one is created.
    model : str, optional
        LLM model identifier for Cypher generation.
    temperature : float, optional
        Sampling temperature for generation.
    k : int, optional
        Number of few-shot examples to retrieve.

    Examples
    --------
    >>> with Neo4jConnection() as conn:
    ...     orch = GraphRAGOrchestrator(
    ...         connection=conn,
    ...         llm_client=hf_client,
    ...         openai_api_key="sk-...",
    ...     )
    ...     result = orch.run_pipeline("Show all down routers and their impact")
    ...     print(result["final_report"])
    """

    def __init__(
        self,
        connection: Neo4jConnection,
        llm_client: OpenAI,
        openai_api_key: str,
        synthesis_agent: Optional[SynthesisAgent] = None,
        vector_memory: Optional[CypherVectorMemory] = None,
        model: str = "Qwen/Qwen2.5-7B-Instruct",
        temperature: float = 0.0,
        k: int = 3,
    ) -> None:
        self._connection: Neo4jConnection = connection
        self._llm_client: OpenAI = llm_client
        self._model: str = model
        self._temperature: float = temperature
        self._k: int = k

        # Synthesis agent (Pillar: Report Generation)
        self._synthesis_agent: SynthesisAgent = synthesis_agent or SynthesisAgent(
            client=llm_client,
            model=model,
            temperature=0.3,
        )

        # Vector memory (Pillar 1: Dynamic Vector Routing)
        self._vector_memory: CypherVectorMemory = vector_memory or CypherVectorMemory(
            openai_api_key=openai_api_key,
        )
        self._vector_memory.populate()

        # Compile the LangGraph state machine
        self._app = self._build_graph()

        logger.info(
            "GraphRAGOrchestrator initialised as LangGraph StateGraph "
            "(model=%s, k=%d, max_iterations=%d)",
            self._model,
            self._k,
            MAX_ITERATIONS,
        )

    # ------------------------------------------------------------------
    # Public API (preserved for backward compatibility)
    # ------------------------------------------------------------------

    def run_pipeline(self, user_query: str) -> Dict[str, Any]:
        """Execute the full GraphRAG pipeline for *user_query*.

        Invokes the compiled LangGraph state machine which cycles
        through retrieval → generation → Neo4j execution (with up to
        MAX_ITERATIONS retries on error) → synthesis.

        Parameters
        ----------
        user_query : str
            A natural-language question about the telecom knowledge
            graph.

        Returns
        -------
        Dict[str, Any]
            A dictionary with the following keys:

            * ``user_query``        – the original question.
            * ``generated_cypher``  – the final Cypher string.
            * ``graph_data_length`` – number of records returned.
            * ``graph_data``        – the raw Neo4j records.
            * ``final_report``      – the synthesised report.
            * ``iteration_count``   – number of generation cycles used.
        """
        initial_state: GraphState = {
            "user_query": user_query,
            "chat_history": [],
            "retrieved_schema": {},
            "few_shot_examples": [],
            "generated_cypher": "",
            "graph_data": [],
            "error_feedback": "",
            "iteration_count": 0,
            "final_report": "",
        }

        logger.info("Pipeline started for query: %s", user_query)

        try:
            final_state: GraphState = self._app.invoke(initial_state)

            logger.info(
                "Pipeline completed (iterations=%d, records=%d)",
                final_state["iteration_count"],
                len(final_state["graph_data"]),
            )

            return {
                "user_query": user_query,
                "generated_cypher": final_state["generated_cypher"],
                "graph_data_length": len(final_state["graph_data"]),
                "graph_data": final_state["graph_data"],
                "final_report": final_state["final_report"],
                "iteration_count": final_state["iteration_count"],
            }

        except Exception as exc:
            logger.exception("GraphRAG pipeline failed for: %s", user_query)
            return {
                "user_query": user_query,
                "generated_cypher": "",
                "graph_data_length": 0,
                "graph_data": [],
                "final_report": (
                    f"[ERROR] GraphRAG pipeline raised an exception: {exc}"
                ),
                "iteration_count": 0,
                "generated_answer": f"[ERROR] GraphRAG pipeline raised an exception: {exc}",
                "retrieved_contexts": [
                    f'{{"error_type": "{type(exc).__name__}", '
                    f'"error_message": "{str(exc)}", '
                    f'"traceback": "{traceback.format_exc()[-500:]}"}}'
                ],
                "latency_seconds": 0.0,
            }

    # ------------------------------------------------------------------
    # LangGraph Construction
    # ------------------------------------------------------------------

    def _build_graph(self) -> Any:
        """Construct and compile the LangGraph StateGraph.

        Graph topology::

            retrieve_context → generate_cypher → execute_neo4j
                                     ↑                  │
                                     └── (on error) ────┘
                                                        │ (on success)
                                                        ▼
                                                   synthesize → END

        Returns
        -------
        CompiledStateGraph
            The compiled LangGraph application.
        """
        workflow = StateGraph(GraphState)

        # Register nodes
        workflow.add_node("retrieve_context", self._node_retrieve_context)
        workflow.add_node("generate_cypher", self._node_generate_cypher)
        workflow.add_node("execute_neo4j", self._node_execute_neo4j)
        workflow.add_node("synthesize", self._node_synthesize)

        # Define edge topology
        workflow.set_entry_point("retrieve_context")
        workflow.add_edge("retrieve_context", "generate_cypher")
        workflow.add_edge("generate_cypher", "execute_neo4j")

        # Conditional edge: execute_neo4j → synthesize or → generate_cypher
        workflow.add_conditional_edges(
            "execute_neo4j",
            self._route_after_execution,
            {
                "synthesize": "synthesize",
                "retry": "generate_cypher",
            },
        )

        workflow.add_edge("synthesize", END)

        return workflow.compile()

    # ------------------------------------------------------------------
    # Node 1: Retrieve Context (Dynamic Vector Routing)
    # ------------------------------------------------------------------

    def _node_retrieve_context(self, state: GraphState) -> Dict[str, Any]:
        """Retrieve semantically relevant few-shot examples from ChromaDB.

        This node implements **Pillar 1 (Dynamic Vector Routing)** and
        **Pillar 2 (Dynamic Context Assembly)**.  The user's query is
        embedded and compared against the indexed Cypher Query Library.
        The retrieved examples' partial schemas are unioned to produce
        the minimal context for generation.

        Parameters
        ----------
        state : GraphState
            Current pipeline state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``retrieved_schema``, ``few_shot_examples``.
        """
        logger.info(
            "[Node: retrieve_context] Querying ChromaDB for k=%d examples",
            self._k,
        )

        context: RetrievedContext = self._vector_memory.retrieve(
            user_prompt=state["user_query"],
            k=self._k,
        )

        # Convert schema_union sets to sorted lists for serialisability
        schema_dict: Dict[str, Any] = {
            key: sorted(values) for key, values in context.schema_union.items()
        }

        logger.info(
            "[Node: retrieve_context] Retrieved %d examples. Schema union: "
            "layers=%s, relationships=%s",
            len(context.few_shot_examples),
            schema_dict.get("layers", []),
            schema_dict.get("relationships", []),
        )

        return {
            "retrieved_schema": schema_dict,
            "few_shot_examples": context.few_shot_examples,
        }

    # ------------------------------------------------------------------
    # Node 2: Generate Cypher (LLM Call with Dynamic Context)
    # ------------------------------------------------------------------

    def _node_generate_cypher(self, state: GraphState) -> Dict[str, Any]:
        """Generate a Cypher query using the LLM with dynamic context.

        Assembles the system prompt by injecting:
        - The dynamically computed schema union (Pillar 2)
        - The retrieved few-shot examples (Pillar 1)
        - Any error feedback from a previous failed Neo4j execution

        Increments ``iteration_count`` on each call.

        Parameters
        ----------
        state : GraphState
            Current pipeline state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``generated_cypher``, ``iteration_count``.
        """
        iteration = state["iteration_count"] + 1
        logger.info(
            "[Node: generate_cypher] Iteration %d/%d",
            iteration,
            MAX_ITERATIONS,
        )

        # Assemble dynamic schema block from retrieved context
        schema = state["retrieved_schema"]
        dynamic_schema_block = (
            "## DYNAMICALLY ASSEMBLED SCHEMA (union of retrieved examples)\n"
            f"Layers: {', '.join(schema.get('layers', []))}\n"
            f"Node Types: {', '.join(schema.get('node_types', [])) or 'any TelecomNode'}\n"
            f"Relationships: {', '.join(schema.get('relationships', []))}\n"
            f"Properties: {', '.join(schema.get('properties_used', []))}"
        )

        # Assemble dynamic few-shot examples block
        examples = state["few_shot_examples"]
        example_lines = ["## DYNAMICALLY RETRIEVED FEW-SHOT EXAMPLES"]
        for i, ex in enumerate(examples, 1):
            example_lines.append(f"\n### Example {i}")
            example_lines.append(f"Question: {ex['question']}")
            example_lines.append(f"Cypher:\n{ex['cypher']}")
        dynamic_examples_block = "\n".join(example_lines)

        # Build full system prompt with dynamic injections
        system_prompt = _BASE_SYSTEM_PROMPT.format(
            dynamic_schema_block=dynamic_schema_block,
            dynamic_examples_block=dynamic_examples_block,
        )

        # Build user message — include error feedback on retry iterations
        user_message = state["user_query"]
        if state["error_feedback"]:
            user_message += _ERROR_FEEDBACK_ADDENDUM.format(
                error=state["error_feedback"]
            )
            logger.info(
                "[Node: generate_cypher] Appending error feedback: %s",
                state["error_feedback"][:200],
            )

        # Call LLM via OpenAI SDK (HuggingFace Serverless Inference)
        response = self._llm_client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=self._temperature,
        )

        raw: str = response.choices[0].message.content or ""
        cypher: str = self._clean_response(raw)

        logger.info("[Node: generate_cypher] Generated Cypher: %s", cypher)

        return {
            "generated_cypher": cypher,
            "iteration_count": iteration,
        }

    # ------------------------------------------------------------------
    # Node 3: Execute Neo4j (Database Execution with Error Capture)
    # ------------------------------------------------------------------

    def _node_execute_neo4j(self, state: GraphState) -> Dict[str, Any]:
        """Execute the generated Cypher against Neo4j.

        This node implements the **database-level validation** that
        distinguishes the thesis architecture from a naive pipeline.
        It catches:

        1. **Neo4j syntax/semantic errors** — captured as
           ``error_feedback`` for the retry loop.
        2. **Empty result sets** (0 rows) — treated as a soft error,
           prompting the LLM to broaden the query.
        3. **Security violations** — blocked write queries caught by
           the sanitizer.

        On success, ``graph_data`` is populated and ``error_feedback``
        is cleared, signalling the conditional edge to route to
        synthesis.

        Parameters
        ----------
        state : GraphState
            Current pipeline state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``graph_data``, ``error_feedback``.
        """
        cypher = state["generated_cypher"]
        logger.info(
            "[Node: execute_neo4j] Executing Cypher (iteration %d): %s",
            state["iteration_count"],
            cypher,
        )

        # Guard: empty Cypher from LLM
        if not cypher or not cypher.strip():
            logger.warning("[Node: execute_neo4j] Empty Cypher — setting error.")
            return {
                "graph_data": [],
                "error_feedback": (
                    "The LLM returned an empty response instead of a Cypher query. "
                    "Generate a valid MATCH ... RETURN ... LIMIT 25 query."
                ),
            }

        # Attempt execution against Neo4j
        try:
            results: List[Dict[str, Any]] = self._connection.execute_llm_read_query(
                cypher,
            )
        except Exception as exc:
            # Capture the full error string for LLM feedback
            error_msg = f"{type(exc).__name__}: {str(exc)}"
            logger.warning(
                "[Node: execute_neo4j] Execution FAILED — %s", error_msg
            )
            return {
                "graph_data": [],
                "error_feedback": error_msg,
            }

        # Check for empty results (soft error)
        if not results:
            logger.warning(
                "[Node: execute_neo4j] Query returned 0 results."
            )
            return {
                "graph_data": [],
                "error_feedback": (
                    "Query returned 0 results. Broaden the search by removing "
                    "specific name filters or relaxing status constraints. "
                    "Try matching on node_type alone without status filters."
                ),
            }

        # Success path
        logger.info(
            "[Node: execute_neo4j] SUCCESS — %d record(s) returned.",
            len(results),
        )
        return {
            "graph_data": results,
            "error_feedback": "",
        }

    # ------------------------------------------------------------------
    # Node 4: Synthesize (Report Generation)
    # ------------------------------------------------------------------

    def _node_synthesize(self, state: GraphState) -> Dict[str, Any]:
        """Generate the Operational Intelligence report.

        If ``graph_data`` is empty (max iterations exhausted), the
        node produces a graceful failure message explaining that the
        query could not be resolved within the iteration budget.

        Parameters
        ----------
        state : GraphState
            Current pipeline state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``final_report``.
        """
        logger.info(
            "[Node: synthesize] Generating report (records=%d, iterations=%d)",
            len(state["graph_data"]),
            state["iteration_count"],
        )

        # Graceful failure: no data after max iterations
        if not state["graph_data"]:
            failure_report = (
                "## Query Resolution Failed\n\n"
                f"The system attempted {state['iteration_count']} Cypher "
                f"generation cycles but could not retrieve data from the "
                f"knowledge graph.\n\n"
                f"**Last generated Cypher:**\n```cypher\n"
                f"{state['generated_cypher']}\n```\n\n"
                f"**Last error:** {state['error_feedback']}\n\n"
                f"**Recommendation:** Rephrase the question with more "
                f"specific node names or broader search criteria."
            )
            logger.warning("[Node: synthesize] Graceful failure — no data.")
            return {"final_report": failure_report}

        # Normal synthesis with graph data
        report: str = self._synthesis_agent.generate_report(
            user_prompt=state["user_query"],
            graph_context=state["graph_data"],
        )

        logger.info(
            "[Node: synthesize] Report generated (%d chars).", len(report)
        )
        return {"final_report": report}

    # ------------------------------------------------------------------
    # Conditional Edge Router (The Feedback Loop)
    # ------------------------------------------------------------------

    def _route_after_execution(self, state: GraphState) -> str:
        """Route after Neo4j execution: retry generation or synthesize.

        This conditional edge implements the **cyclical feedback loop**
        that is the core architectural contribution of the Dynamic
        GraphRAG system:

        - If ``graph_data`` is populated (success) → ``"synthesize"``
        - If error AND ``iteration_count < MAX_ITERATIONS`` → ``"retry"``
          (loop back to generate_cypher with error context)
        - If error AND ``iteration_count >= MAX_ITERATIONS`` →
          ``"synthesize"`` (graceful failure report)

        Parameters
        ----------
        state : GraphState
            Current pipeline state after Neo4j execution.

        Returns
        -------
        str
            Route key: ``"synthesize"`` or ``"retry"``.
        """
        # Success: data was retrieved from Neo4j
        if state["graph_data"]:
            logger.info(
                "[Router] Neo4j returned data → routing to synthesize."
            )
            return "synthesize"

        # Error: check remaining iteration budget
        if state["iteration_count"] >= MAX_ITERATIONS:
            logger.error(
                "[Router] Max iterations (%d) exhausted. Error: %s → "
                "routing to synthesize (graceful failure).",
                MAX_ITERATIONS,
                state["error_feedback"][:100],
            )
            return "synthesize"

        # Error with budget remaining: retry with feedback
        logger.info(
            "[Router] Execution failed (iteration %d/%d). Error: %s → "
            "routing to retry.",
            state["iteration_count"],
            MAX_ITERATIONS,
            state["error_feedback"][:100],
        )
        return "retry"

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_response(raw: str) -> str:
        """Strip markdown fences, trailing semicolons, and whitespace."""
        cleaned: str = _FENCE_RE.sub("", raw).strip()
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1].rstrip()
        return cleaned
    
    
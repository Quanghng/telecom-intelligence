"""
graph_orchestrator.py – Cyclical LangGraph State Machine for Cypher Generation
================================================================================
Implements **Pillar 3 (Cyclical State Machine Orchestration)** of the
Dynamic GraphRAG architecture using LangGraph's ``StateGraph``.

Architecture Overview
---------------------
Traditional single-shot LLM pipelines suffer from brittleness: a
malformed Cypher query terminates the entire pipeline.  The naive
``generate_query_with_fallback`` pattern (one retry with a relaxed
prompt) provides minimal resilience.

This module replaces that approach with a **formal cyclical state
machine** modelled as a directed graph of processing nodes:

.. code-block:: text

    ┌────────────────────────────────────────────────────────────┐
    │                    START                                    │
    └────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────────────┐
    │  Node 1: VECTOR RETRIEVAL                                  │
    │  • Embed user prompt via text-embedding-3-small            │
    │  • Query ChromaDB for k=3 most similar examples            │
    │  • Compute schema union (Dynamic Context Assembly)          │
    └────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────────────┐
    │  Node 2: CYPHER GENERATION                                 │
    │  • Assemble dynamic prompt (schema union + few-shot)        │
    │  • Call HuggingFace LLM via OpenAI SDK                     │
    │  • Strip markdown fences, clean output                      │
    └────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────────────┐
    │  Node 3: VALIDATION                                        │
    │  • Check for empty string                                  │
    │  • Reject variable-length paths / shortestPath             │
    │  • Reject write/mutation keywords                           │
    │  • Basic syntax heuristics                                  │
    └───────────┬────────────────────────────┬───────────────────┘
                │ PASS                       │ FAIL
                ▼                            ▼
    ┌───────────────────┐        ┌───────────────────────────────┐
    │       END         │        │  Conditional Edge:            │
    │  (return cypher)  │        │  if iteration < 3 → Node 2   │
    └───────────────────┘        │  else → END (graceful fail)   │
                                 └───────────────────────────────┘

The cyclical edge from Validation back to Generation allows the system
to self-correct by appending validation error messages to the prompt,
giving the LLM explicit feedback on what went wrong.  The strict
iteration cap (``MAX_ITERATIONS = 3``) prevents infinite loops and
ensures bounded latency.

State Definition
----------------
The state is a ``TypedDict`` (``CypherGenState``) containing:

- ``user_prompt``: The original natural-language question.
- ``retrieved_context``: The ``RetrievedContext`` from ChromaDB.
- ``generated_cypher``: The current Cypher candidate.
- ``validation_errors``: List of errors from the validation node.
- ``iteration_count``: Number of generation attempts.
- ``is_valid``: Whether the current candidate passed validation.

Usage
-----
    >>> from openai import OpenAI
    >>> from src.llm.graph_orchestrator import CypherGraphOrchestrator
    >>> client = OpenAI(base_url="https://router.huggingface.co/v1", api_key="hf_...")
    >>> orchestrator = CypherGraphOrchestrator(
    ...     llm_client=client,
    ...     openai_api_key="sk-...",
    ... )
    >>> result = orchestrator.invoke("Show me all down routers")
    >>> print(result["generated_cypher"])
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set

from langgraph.graph import END, StateGraph
from openai import OpenAI
from typing_extensions import TypedDict

from src.llm.vector_memory import CypherVectorMemory, RetrievedContext

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

MAX_ITERATIONS: int = 3
"""Maximum number of generation→validation cycles before graceful failure."""

# Regex to strip markdown code fences the LLM sometimes adds
_FENCE_RE: re.Pattern[str] = re.compile(
    r"^```(?:cypher|sql|plaintext)?\s*\n?|```\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Regex to detect forbidden variable-length relationship patterns
_VAR_LENGTH_RE: re.Pattern[str] = re.compile(
    r"\[:\w+\*[\d.]*\]|\[\*\]|shortestPath\s*\(",
    re.IGNORECASE,
)

# Regex to detect write/mutation keywords (safety constraint)
_WRITE_KEYWORDS_RE: re.Pattern[str] = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL\s*\{)\b",
    re.IGNORECASE,
)


# ------------------------------------------------------------------
# State Definition (TypedDict for LangGraph)
# ------------------------------------------------------------------


class CypherGenState(TypedDict):
    """Typed state container for the LangGraph Cypher generation machine.

    This TypedDict defines the complete state flowing through the
    cyclical state graph.  Each node reads from and writes to this
    shared state, enabling the validation→regeneration feedback loop.

    Attributes
    ----------
    user_prompt : str
        The original natural-language telecom question.
    retrieved_context : Optional[RetrievedContext]
        Dynamic context retrieved from ChromaDB (few-shot examples
        and schema union). Populated by the retrieval node.
    generated_cypher : str
        The current Cypher query candidate. Updated on each
        generation cycle.
    validation_errors : List[str]
        Accumulated error messages from the validation node.
        These are appended to the generation prompt on retry,
        providing explicit feedback to the LLM.
    iteration_count : int
        Number of generation attempts. Used by the conditional
        edge to enforce the MAX_ITERATIONS bound.
    is_valid : bool
        Whether the current Cypher candidate passed all validation
        checks.  Drives the conditional routing at the graph edge.
    """

    user_prompt: str
    retrieved_context: Optional[RetrievedContext]
    generated_cypher: str
    validation_errors: List[str]
    iteration_count: int
    is_valid: bool


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

_RETRY_ADDENDUM: str = """\

## PREVIOUS ATTEMPT FAILED — CORRECT THESE ERRORS:
{errors}

Generate a CORRECTED Cypher query that avoids these issues.
"""


# ------------------------------------------------------------------
# CypherGraphOrchestrator – LangGraph-based Agent
# ------------------------------------------------------------------


class CypherGraphOrchestrator:
    """LangGraph-based cyclical state machine for Cypher generation.

    This orchestrator implements the three architectural pillars of
    the Dynamic GraphRAG system:

    1. **Dynamic Vector Routing** — queries ChromaDB for semantically
       relevant few-shot examples.
    2. **Dynamic Context Assembly** — computes the schema union of
       retrieved examples to build a minimal, query-specific prompt.
    3. **Cyclical State Machine** — uses a LangGraph StateGraph with
       a validation→retry loop bounded by MAX_ITERATIONS.

    Parameters
    ----------
    llm_client : OpenAI
        An already-initialised OpenAI-compatible client pointed at
        the HuggingFace Serverless Inference API (or any OpenAI-
        compatible endpoint).
    openai_api_key : str
        API key for OpenAI embeddings (text-embedding-3-small).
    model : str, optional
        Model identifier for Cypher generation.
    temperature : float, optional
        Sampling temperature for generation.
    k : int, optional
        Number of few-shot examples to retrieve from ChromaDB.
    vector_memory : CypherVectorMemory | None, optional
        Pre-initialised vector memory instance.  If None, one is
        created using the provided ``openai_api_key``.

    Examples
    --------
    >>> from openai import OpenAI
    >>> client = OpenAI(
    ...     base_url="https://router.huggingface.co/v1",
    ...     api_key="hf_..."
    ... )
    >>> orchestrator = CypherGraphOrchestrator(
    ...     llm_client=client,
    ...     openai_api_key="sk-...",
    ...     model="Qwen/Qwen2.5-7B-Instruct",
    ... )
    >>> result = orchestrator.invoke("Show all down Core_Routers")
    >>> print(result["generated_cypher"])
    """

    def __init__(
        self,
        llm_client: OpenAI,
        openai_api_key: str,
        model: str = "Qwen/Qwen2.5-7B-Instruct",
        temperature: float = 0.0,
        k: int = 3,
        vector_memory: CypherVectorMemory | None = None,
    ) -> None:
        self._llm_client: OpenAI = llm_client
        self._model: str = model
        self._temperature: float = temperature
        self._k: int = k

        # Initialise vector memory (Pillar 1)
        self._vector_memory: CypherVectorMemory = vector_memory or CypherVectorMemory(
            openai_api_key=openai_api_key,
        )

        # Ensure collection is populated
        self._vector_memory.populate()

        # Compile the LangGraph state machine
        self._graph = self._build_graph()

        logger.info(
            "CypherGraphOrchestrator initialised (model=%s, k=%d, "
            "max_iterations=%d)",
            self._model,
            self._k,
            MAX_ITERATIONS,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def invoke(self, user_prompt: str) -> CypherGenState:
        """Execute the full cyclical generation pipeline.

        This is the primary entry point.  It initialises the state
        and invokes the compiled LangGraph, which will cycle through
        retrieval → generation → validation (with up to
        MAX_ITERATIONS retries) and return the final state.

        Parameters
        ----------
        user_prompt : str
            Natural-language telecom question.

        Returns
        -------
        CypherGenState
            The terminal state containing the generated Cypher (or
            an error message if all iterations failed).
        """
        initial_state: CypherGenState = {
            "user_prompt": user_prompt,
            "retrieved_context": None,
            "generated_cypher": "",
            "validation_errors": [],
            "iteration_count": 0,
            "is_valid": False,
        }

        logger.info("Invoking LangGraph pipeline for: %s", user_prompt)
        final_state: CypherGenState = self._graph.invoke(initial_state)

        if final_state["is_valid"]:
            logger.info(
                "Pipeline succeeded after %d iteration(s): %s",
                final_state["iteration_count"],
                final_state["generated_cypher"],
            )
        else:
            logger.error(
                "Pipeline FAILED after %d iteration(s). Errors: %s",
                final_state["iteration_count"],
                final_state["validation_errors"],
            )

        return final_state

    def generate_query(self, user_prompt: str) -> str:
        """Convenience method matching the original CypherGenerationAgent API.

        Parameters
        ----------
        user_prompt : str
            Natural-language telecom question.

        Returns
        -------
        str
            The generated Cypher query.

        Raises
        ------
        ValueError
            If the pipeline fails to produce a valid query after
            MAX_ITERATIONS attempts.
        """
        result = self.invoke(user_prompt)
        if result["is_valid"]:
            return result["generated_cypher"]
        raise ValueError(
            f"Cypher generation failed after {MAX_ITERATIONS} attempts. "
            f"Errors: {result['validation_errors']}"
        )

    # ------------------------------------------------------------------
    # LangGraph Construction
    # ------------------------------------------------------------------

    def _build_graph(self) -> Any:
        """Construct and compile the LangGraph StateGraph.

        The graph topology is:

            vector_retrieval → cypher_generation → validation
                                      ↑                │
                                      └── (on fail) ───┘

        With a conditional edge from validation that either routes to
        END (on success or max iterations) or back to generation
        (on failure with iterations remaining).

        Returns
        -------
        CompiledStateGraph
            The compiled LangGraph ready for invocation.
        """
        # Define the state graph
        workflow = StateGraph(CypherGenState)

        # Add nodes
        workflow.add_node("vector_retrieval", self._node_vector_retrieval)
        workflow.add_node("cypher_generation", self._node_cypher_generation)
        workflow.add_node("validation", self._node_validation)

        # Define edges
        workflow.set_entry_point("vector_retrieval")
        workflow.add_edge("vector_retrieval", "cypher_generation")
        workflow.add_edge("cypher_generation", "validation")

        # Conditional edge: validation → END or → cypher_generation
        workflow.add_conditional_edges(
            "validation",
            self._route_after_validation,
            {
                "end": END,
                "retry": "cypher_generation",
            },
        )

        return workflow.compile()

    # ------------------------------------------------------------------
    # Graph Nodes
    # ------------------------------------------------------------------

    def _node_vector_retrieval(self, state: CypherGenState) -> Dict[str, Any]:
        """Node 1: Dynamic Vector Retrieval from ChromaDB.

        Embeds the user's prompt and retrieves the k most semantically
        similar few-shot examples from the Cypher Query Library.
        Computes the schema union for Dynamic Context Assembly.

        Parameters
        ----------
        state : CypherGenState
            Current graph state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``retrieved_context``.
        """
        logger.info("[Node: vector_retrieval] Querying ChromaDB for k=%d", self._k)
        context: RetrievedContext = self._vector_memory.retrieve(
            user_prompt=state["user_prompt"],
            k=self._k,
        )
        return {"retrieved_context": context}

    def _node_cypher_generation(self, state: CypherGenState) -> Dict[str, Any]:
        """Node 2: LLM-based Cypher Generation with dynamic context.

        Assembles the system prompt by injecting:
        - The dynamically computed schema union (Pillar 2)
        - The retrieved few-shot examples (Pillar 1)
        - Any validation errors from previous iterations (Pillar 3)

        Calls the HuggingFace LLM via the OpenAI SDK.

        Parameters
        ----------
        state : CypherGenState
            Current graph state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``generated_cypher``, ``iteration_count``.
        """
        iteration = state["iteration_count"] + 1
        logger.info(
            "[Node: cypher_generation] Iteration %d/%d",
            iteration,
            MAX_ITERATIONS,
        )

        # Assemble dynamic prompt components
        context: RetrievedContext = state["retrieved_context"]
        dynamic_schema = context.format_schema_block()
        dynamic_examples = context.format_examples_block()

        system_prompt = _BASE_SYSTEM_PROMPT.format(
            dynamic_schema_block=dynamic_schema,
            dynamic_examples_block=dynamic_examples,
        )

        # Build user message (include error feedback on retries)
        user_message = state["user_prompt"]
        if state["validation_errors"]:
            error_feedback = "\n".join(
                f"- {err}" for err in state["validation_errors"]
            )
            user_message += _RETRY_ADDENDUM.format(errors=error_feedback)

        # Call LLM
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

        logger.info("[Node: cypher_generation] Raw output: %s", cypher)

        return {
            "generated_cypher": cypher,
            "iteration_count": iteration,
        }

    def _node_validation(self, state: CypherGenState) -> Dict[str, Any]:
        """Node 3: Cypher Validation with multi-rule checking.

        Applies a battery of validation rules to the generated Cypher:

        1. **Non-empty check** — rejects empty or whitespace-only output.
        2. **Variable-length path detection** — rejects ``[:REL*]``,
           ``shortestPath()``, and bounded patterns.
        3. **Write-keyword detection** — rejects any mutation keywords
           (CREATE, MERGE, DELETE, SET, REMOVE, DROP).
        4. **Basic syntax heuristics** — checks for MATCH and RETURN
           keywords (minimal syntactic sanity).

        Parameters
        ----------
        state : CypherGenState
            Current graph state.

        Returns
        -------
        Dict[str, Any]
            State updates: ``is_valid``, ``validation_errors``.
        """
        cypher = state["generated_cypher"]
        errors: List[str] = []

        logger.info("[Node: validation] Validating Cypher (iteration %d)", state["iteration_count"])

        # Rule 1: Non-empty
        if not cypher or not cypher.strip():
            errors.append("Generated Cypher is empty or whitespace-only.")

        # Rule 2: Variable-length paths
        if cypher and _VAR_LENGTH_RE.search(cypher):
            errors.append(
                "Contains forbidden variable-length path pattern "
                "(e.g., [:REL*], shortestPath). Use explicit single-hop "
                "patterns chained together."
            )

        # Rule 3: Write/mutation keywords
        if cypher and _WRITE_KEYWORDS_RE.search(cypher):
            errors.append(
                "Contains write/mutation keywords (CREATE, MERGE, DELETE, "
                "SET, REMOVE, DROP). Only read-only queries are permitted."
            )

        # Rule 4: Basic syntax (must contain MATCH and RETURN)
        if cypher and not re.search(r"\bMATCH\b", cypher, re.IGNORECASE):
            errors.append("Missing MATCH clause — not a valid Cypher read query.")
        if cypher and not re.search(r"\bRETURN\b", cypher, re.IGNORECASE):
            errors.append("Missing RETURN clause — query must return results.")

        is_valid = len(errors) == 0

        if is_valid:
            logger.info("[Node: validation] PASSED ✓")
        else:
            logger.warning("[Node: validation] FAILED ✗ — %s", errors)

        return {
            "is_valid": is_valid,
            "validation_errors": errors,
        }

    # ------------------------------------------------------------------
    # Conditional Edge Router
    # ------------------------------------------------------------------

    @staticmethod
    def _route_after_validation(state: CypherGenState) -> str:
        """Conditional edge: decide whether to retry or terminate.

        Routing logic:
        - If valid → ``"end"`` (proceed to graph END node)
        - If invalid AND iterations < MAX_ITERATIONS → ``"retry"``
          (loop back to cypher_generation)
        - If invalid AND iterations >= MAX_ITERATIONS → ``"end"``
          (graceful failure, return accumulated errors)

        Parameters
        ----------
        state : CypherGenState
            Current graph state after validation.

        Returns
        -------
        str
            Route key: ``"end"`` or ``"retry"``.
        """
        if state["is_valid"]:
            return "end"

        if state["iteration_count"] >= MAX_ITERATIONS:
            logger.error(
                "Max iterations (%d) reached. Returning graceful failure.",
                MAX_ITERATIONS,
            )
            return "end"

        logger.info(
            "Validation failed (iteration %d/%d) — routing to retry.",
            state["iteration_count"],
            MAX_ITERATIONS,
        )
        return "retry"

    # ------------------------------------------------------------------
    # Utility Methods
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_response(raw: str) -> str:
        """Strip markdown fences, trailing semicolons, and whitespace.

        Parameters
        ----------
        raw : str
            Raw LLM output.

        Returns
        -------
        str
            Cleaned Cypher string.
        """
        cleaned: str = _FENCE_RE.sub("", raw).strip()
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1].rstrip()
        return cleaned

"""
runner.py – Evaluation Runner & Benchmarking Engine
=====================================================
Executes the curated ground-truth test cases from
:mod:`src.evaluation.seed_data` against **both** RAG pipelines
(Vector RAG and GraphRAG) and records structured results for
downstream RAGAS metric computation.

Key responsibilities
--------------------
1. Run each :class:`~src.evaluation.schema.EvaluationTestCase` through
   the **GraphRAG** orchestrator and the **Vector RAG** retriever.
2. Measure wall-clock latency via :func:`time.perf_counter`.
3. Package every execution into an :class:`EvaluationResult` Pydantic
   model for type-safe downstream consumption.
4. Export the full result set to ``data/evaluation_results.csv`` for
   thesis plotting and statistical analysis.

Design notes
------------
- The Vector RAG arm uses the :class:`~src.baseline.vector_retriever.VectorRetriever`
  for context retrieval and a :class:`~src.llm.summary_agent.SynthesisAgent`
  to generate a natural-language answer from the retrieved chunks.
  This mirrors the GraphRAG pipeline's two-stage design (retrieve → synthesise)
  so that both arms are evaluated on the same footing.
- RAGAS metric calculation is **not** implemented here; this module
  focuses exclusively on execution, data formatting, and latency
  benchmarking.

Usage
-----
    >>> from src.evaluation.runner import EvaluationRunner
    >>> from src.evaluation.seed_data import get_thesis_test_cases
    >>> runner = EvaluationRunner(graph_rag=orchestrator, vector_retriever=retriever, synthesis_agent=agent)
    >>> results = runner.run_all(get_thesis_test_cases())
    >>> df = runner.export_to_pandas(results)
    >>> df.shape
    (20, 9)
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Final, List, Literal

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from src.baseline.vector_retriever import VectorRetriever
from src.core.orchestrator import GraphRAGOrchestrator
from src.evaluation.schema import EvaluationTestCase
from src.llm.summary_agent import SynthesisAgent

logger: logging.Logger = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_DEFAULT_EXPORT_PATH: Final[Path] = _PROJECT_ROOT / "data" / "evaluation_results.csv"

# Strict literal type for pipeline identification
PipelineType = Literal["VectorRAG", "GraphRAG"]


# =============================================================================
# Pydantic result model
# =============================================================================

class EvaluationResult(BaseModel):
    """Stores a single test-case execution against one RAG pipeline.

    Every field required for downstream RAGAS computation and thesis
    plotting is captured here.  The model is intentionally flat so that
    it maps 1-to-1 to a CSV / DataFrame row.

    Attributes
    ----------
    query_id : str
        Unique test-case identifier inherited from the seed data.
    question : str
        The natural-language question that was posed.
    ground_truth : str
        The pre-verified correct answer (reference for RAGAS metrics).
    hop_depth : int
        Minimum graph hops required to answer the question (1–5).
    category : str
        Semantic evaluation category (e.g. ``"Blast Radius"``).
    pipeline_type : ``"VectorRAG"`` | ``"GraphRAG"``
        Which pipeline produced this result.
    generated_answer : str
        The final LLM-synthesised answer.
    retrieved_contexts : List[str]
        Raw text chunks (Vector RAG) or stringified graph JSON
        (GraphRAG) that were fed to the synthesis stage.
    latency_seconds : float
        Wall-clock execution time measured with
        :func:`time.perf_counter`.

    Examples
    --------
    >>> result = EvaluationResult(
    ...     query_id="TC-001",
    ...     question="Which Core_Routers are Down?",
    ...     ground_truth="CR-01 is Down.",
    ...     hop_depth=1,
    ...     category="Status Lookup",
    ...     pipeline_type="GraphRAG",
    ...     generated_answer="Core_Router CR-01 is currently Down.",
    ...     retrieved_contexts=["{'node_id': 'CR-01', 'status': 'Down'}"],
    ...     latency_seconds=1.234,
    ... )
    """

    query_id: str = Field(
        ..., min_length=1, description="Test-case identifier."
    )
    question: str = Field(
        ..., min_length=10, description="Natural-language question."
    )
    ground_truth: str = Field(
        ..., min_length=10, description="Pre-verified correct answer."
    )
    hop_depth: int = Field(
        ..., ge=1, le=5, description="Minimum graph hops required."
    )
    category: str = Field(
        ..., description="Semantic evaluation category."
    )
    pipeline_type: PipelineType = Field(
        ..., description="Pipeline that produced the result: 'VectorRAG' or 'GraphRAG'."
    )
    generated_answer: str = Field(
        ..., description="Final LLM-synthesised answer."
    )
    retrieved_contexts: List[str] = Field(
        default_factory=list,
        description="Retrieved text chunks or stringified graph records.",
    )
    latency_seconds: float = Field(
        ..., ge=0.0, description="Execution wall-clock time in seconds."
    )

    # -- Validators -----------------------------------------------------------

    @field_validator("pipeline_type")
    @classmethod
    def _pipeline_type_must_be_valid(cls, v: str) -> str:
        allowed = {"VectorRAG", "GraphRAG"}
        if v not in allowed:
            raise ValueError(
                f"pipeline_type must be one of {allowed}, got '{v}'."
            )
        return v

    # -- Convenience ----------------------------------------------------------

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"EvaluationResult(id={self.query_id!r}, "
            f"pipeline={self.pipeline_type!r}, "
            f"latency={self.latency_seconds:.3f}s)"
        )


# =============================================================================
# Evaluation runner
# =============================================================================

class EvaluationRunner:
    """Executes evaluation test cases against both RAG pipelines.

    The runner drives the curated seed-data test suite through the
    **GraphRAG** orchestrator and the **Vector RAG** retriever,
    records latency, and packages each execution into a validated
    :class:`EvaluationResult`.

    Parameters
    ----------
    graph_rag : GraphRAGOrchestrator
        An already-initialised GraphRAG orchestrator instance whose
        :meth:`~GraphRAGOrchestrator.run_pipeline` method accepts a
        natural-language question and returns a dict with keys
        ``"final_report"`` and ``"graph_data_length"``.
    vector_retriever : VectorRetriever
        An already-initialised Vector RAG retriever whose
        :meth:`~VectorRetriever.retrieve_context` method returns
        ``List[str]`` of similar documents.
    synthesis_agent : SynthesisAgent
        Shared synthesis agent used to generate an LLM answer for
        the Vector RAG arm (the retriever only returns raw chunks).
    export_path : Path, optional
        Destination CSV file for :meth:`export_to_pandas`.  Defaults
        to ``data/evaluation_results.csv``.

    Examples
    --------
    >>> runner = EvaluationRunner(
    ...     graph_rag=orchestrator,
    ...     vector_retriever=retriever,
    ...     synthesis_agent=agent,
    ... )
    >>> results = runner.run_all(get_thesis_test_cases())
    >>> len(results)
    20
    """

    def __init__(
        self,
        graph_rag: GraphRAGOrchestrator,
        vector_retriever: VectorRetriever,
        synthesis_agent: SynthesisAgent,
        export_path: Path = _DEFAULT_EXPORT_PATH,
    ) -> None:
        self._graph_rag: GraphRAGOrchestrator = graph_rag
        self._vector_retriever: VectorRetriever = vector_retriever
        self._synthesis_agent: SynthesisAgent = synthesis_agent
        self._export_path: Path = export_path

        logger.info(
            "EvaluationRunner initialised  |  export_path=%s",
            self._export_path,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_test_case(
        self, test_case: EvaluationTestCase
    ) -> List[EvaluationResult]:
        """Execute *test_case* against **both** pipelines.

        Parameters
        ----------
        test_case : EvaluationTestCase
            A single ground-truth test case.

        Returns
        -------
        List[EvaluationResult]
            Exactly two results — one for GraphRAG, one for VectorRAG —
            ordered ``[GraphRAG, VectorRAG]``.
        """
        logger.info(
            "Running test case %s  |  hops=%d  category=%s",
            test_case.query_id,
            test_case.hop_depth,
            test_case.category,
        )

        graph_result = self._run_graph_rag(test_case)
        vector_result = self._run_vector_rag(test_case)

        return [graph_result, vector_result]

    def run_all(
        self, test_cases: List[EvaluationTestCase]
    ) -> List[EvaluationResult]:
        """Execute every test case in *test_cases* against both pipelines.

        Parameters
        ----------
        test_cases : List[EvaluationTestCase]
            The full evaluation suite (typically from
            :func:`~src.evaluation.seed_data.get_thesis_test_cases`).

        Returns
        -------
        List[EvaluationResult]
            Aggregated results — ``2 * len(test_cases)`` entries.
        """
        logger.info(
            "Starting evaluation run  |  %d test cases × 2 pipelines = %d executions",
            len(test_cases),
            len(test_cases) * 2,
        )

        all_results: List[EvaluationResult] = []

        for idx, tc in enumerate(test_cases, start=1):
            logger.info(
                "Test case %d/%d: %s", idx, len(test_cases), tc.query_id
            )
            pair = self.run_test_case(tc)
            all_results.extend(pair)

            # Log running latency comparison
            graph_latency = pair[0].latency_seconds
            vector_latency = pair[1].latency_seconds
            logger.info(
                "  ├─ GraphRAG  : %.3fs",
                graph_latency,
            )
            logger.info(
                "  └─ VectorRAG : %.3fs  (Δ = %+.3fs)",
                vector_latency,
                vector_latency - graph_latency,
            )
            time.sleep(2)

        logger.info(
            "Evaluation run complete  |  %d total results collected.",
            len(all_results),
        )
        return all_results

    def export_to_pandas(
        self, results: List[EvaluationResult]
    ) -> pd.DataFrame:
        """Convert results to a DataFrame and persist to CSV.

        The ``retrieved_contexts`` list is JSON-serialised into a
        single string column so that the CSV remains parseable.

        If the export CSV already exists, new rows are **appended**
        (without repeating the header) so that incremental ``--hop``
        runs accumulate into a single file.

        Parameters
        ----------
        results : List[EvaluationResult]
            Output of :meth:`run_all` or multiple :meth:`run_test_case`
            calls.

        Returns
        -------
        pandas.DataFrame
            A tidy DataFrame with one row per execution, suitable for
            thesis plotting with *seaborn* / *matplotlib*.
        """
        rows: List[Dict[str, Any]] = []
        for r in results:
            row = r.model_dump()
            # Serialise the list column for CSV compatibility
            row["retrieved_contexts"] = json.dumps(
                row["retrieved_contexts"], ensure_ascii=False
            )
            rows.append(row)

        df = pd.DataFrame(rows)

        # Ensure the parent directory exists
        self._export_path.parent.mkdir(parents=True, exist_ok=True)

        # Append if file already exists; otherwise write with header
        if self._export_path.exists():
            df.to_csv(
                self._export_path, mode="a", header=False, index=False, encoding="utf-8"
            )
            logger.info(
                "Appended %d results to existing %s", len(df), self._export_path
            )
        else:
            df.to_csv(
                self._export_path, mode="w", header=True, index=False, encoding="utf-8"
            )
            logger.info(
                "Exported %d results to %s  |  shape=%s",
                len(df),
                self._export_path,
                df.shape,
            )

        return df

    # ------------------------------------------------------------------
    # Private pipeline executors
    # ------------------------------------------------------------------

    def _run_graph_rag(
        self, test_case: EvaluationTestCase
    ) -> EvaluationResult:
        """Execute *test_case* through the GraphRAG orchestrator.

        Captures the full ``run_pipeline`` output including the
        generated Cypher, graph data, and synthesised report.
        """
        question = test_case.question

        start: float = time.perf_counter()
        try:
            payload: Dict[str, Any] = self._graph_rag.run_pipeline(question)
            generated_answer: str = payload.get("final_report", "")

            # Safely extract and cast Neo4j records to standard dicts.
            # Neo4j driver returns custom Node/Relationship objects that
            # json.dumps cannot serialise — force-cast each record.
            raw_records = payload.get("graph_data", [])
            serialized_records: List[Dict[str, Any]] = []
            for record in raw_records:
                if isinstance(record, dict):
                    clean: Dict[str, Any] = {}
                    for k, v in record.items():
                        # Neo4j Node objects expose dict() or items()
                        if hasattr(v, "items"):
                            clean[k] = dict(v)
                        else:
                            clean[k] = v
                    serialized_records.append(clean)
                else:
                    serialized_records.append({"raw": str(record)})

            # Stringify the pipeline metadata AND the actual graph payload
            context_record: Dict[str, Any] = {
                "generated_cypher": payload.get("generated_cypher", ""),
                "graph_data_length": payload.get("graph_data_length", 0),
                "graph_data": serialized_records,
            }
            retrieved_contexts: List[str] = [
                json.dumps(context_record, default=str)
            ]
        except Exception:
            logger.exception(
                "GraphRAG pipeline failed for %s", test_case.query_id
            )
            generated_answer = "[ERROR] GraphRAG pipeline raised an exception."
            retrieved_contexts = []
        end: float = time.perf_counter()

        latency: float = round(end - start, 6)
        logger.debug(
            "GraphRAG  %s  completed in %.3fs", test_case.query_id, latency
        )

        return EvaluationResult(
            query_id=test_case.query_id,
            question=test_case.question,
            ground_truth=test_case.ground_truth,
            hop_depth=test_case.hop_depth,
            category=test_case.category,
            pipeline_type="GraphRAG",
            generated_answer=generated_answer,
            retrieved_contexts=retrieved_contexts,
            latency_seconds=latency,
        )

    def _run_vector_rag(
        self, test_case: EvaluationTestCase
    ) -> EvaluationResult:
        """Execute *test_case* through the Vector RAG pipeline.

        The Vector RAG arm is a two-stage process:
        1. **Retrieve** – cosine-similarity search via ChromaDB.
        2. **Synthesise** – feed retrieved chunks to the shared
           :class:`SynthesisAgent` so that both arms are evaluated
           on LLM-generated answers (apples-to-apples comparison).
        """
        question = test_case.question

        start: float = time.perf_counter()
        try:
            # Stage 1: Retrieval
            retrieved_contexts: List[str] = (
                self._vector_retriever.retrieve_context(question)
            )

            # Stage 2: Synthesis – convert flat text chunks into the
            # List[Dict] format expected by SynthesisAgent.
            graph_context_proxy: List[Dict[str, Any]] = [
                {"document": doc, "rank": idx}
                for idx, doc in enumerate(retrieved_contexts, start=1)
            ]

            generated_answer: str = self._synthesis_agent.generate_report(
                user_prompt=question,
                graph_context=graph_context_proxy,
            )
        except Exception:
            logger.exception(
                "VectorRAG pipeline failed for %s", test_case.query_id
            )
            generated_answer = "[ERROR] VectorRAG pipeline raised an exception."
            retrieved_contexts = []
        end: float = time.perf_counter()

        latency: float = round(end - start, 6)
        logger.debug(
            "VectorRAG %s  completed in %.3fs", test_case.query_id, latency
        )

        return EvaluationResult(
            query_id=test_case.query_id,
            question=test_case.question,
            ground_truth=test_case.ground_truth,
            hop_depth=test_case.hop_depth,
            category=test_case.category,
            pipeline_type="VectorRAG",
            generated_answer=generated_answer,
            retrieved_contexts=retrieved_contexts,
            latency_seconds=latency,
        )
    
    

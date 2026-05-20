"""
metrics_calculator.py – RAGAS Metric Calculator
=================================================
Wraps the `ragas <https://docs.ragas.io/>`_ evaluation library to score
the structured outputs produced by
:class:`~src.evaluation.runner.EvaluationRunner`.

This module is **Phase 4, Step 4** of the Master's thesis pipeline
comparing **Vector RAG** against **GraphRAG (Neo4j)** for Telecom
Operational Intelligence.

Computed metrics
----------------
- **Faithfulness** – fraction of generated claims supported by the
  retrieved context.
- **Answer Relevancy** – semantic similarity between the generated
  answer and the original question.
- **Context Precision** – ranking quality of retrieved context chunks
  with respect to the ground truth.
- **Context Recall** – coverage of ground-truth facts by the retrieved
  context.

Design notes
------------
- ``evaluator_llm`` and ``evaluator_embeddings`` are LangChain-compatible
  model objects injected at construction time so that the *judge* model
  is decoupled from the *pipeline* model.
- The ``retrieved_contexts`` column produced by
  :meth:`EvaluationRunner.export_to_pandas` is a JSON-encoded string.
  This module gracefully deserialises it back into ``List[str]`` before
  passing it to the HuggingFace :class:`datasets.Dataset`.
- **Rate-limit protection**: evaluation is performed row-by-row with
  configurable inter-row delays and exponential back-off retries to
  guard against HTTP 429 responses from the HuggingFace serverless API.

Usage
-----
    >>> from src.evaluation.metrics_calculator import RagasEvaluator
    >>> evaluator = RagasEvaluator(evaluator_llm=llm, evaluator_embeddings=emb)
    >>> scored_df = evaluator.evaluate_dataframe(runner_df)
    >>> summary  = evaluator.generate_thesis_summary(scored_df)
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Any, Dict, Final, List

import pandas as pd
from datasets import Dataset

from ragas import evaluate
from ragas.metrics import (
    answer_relevancy,
    context_precision,
    context_recall,
    faithfulness,
)

logger: logging.Logger = logging.getLogger(__name__)

# -- RAGAS column mapping -----------------------------------------------------
# Maps our EvaluationRunner DataFrame columns → RAGAS expected names.
_COLUMN_MAP: Final[Dict[str, str]] = {
    "question": "question",
    "generated_answer": "answer",
    "retrieved_contexts": "contexts",
    "ground_truth": "ground_truth",
}

# Metrics that RAGAS will compute
_RAGAS_METRICS: Final[list] = [
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
]

# Numeric columns aggregated in the thesis summary
_SUMMARY_METRIC_COLS: Final[List[str]] = [
    "latency_seconds",
    "faithfulness",
    "answer_relevancy",
    "context_precision",
    "context_recall",
]

# -- Rate-limit protection ----------------------------------------------------
# Delay (in seconds) between consecutive RAGAS per-row evaluations to avoid
# HTTP 429 responses from the HuggingFace serverless inference API.
_INTER_ROW_DELAY_SECONDS: Final[float] = 5.0

# Maximum number of retry attempts for a single row when RAGAS evaluation
# fails due to transient API errors (429, 503, timeouts, etc.).
_MAX_ROW_RETRIES: Final[int] = 3

# Base delay (in seconds) for exponential back-off between retries.
_RETRY_BASE_DELAY_SECONDS: Final[float] = 5.0


# =============================================================================
# Helper utilities
# =============================================================================

def _safe_parse_contexts(raw: Any) -> List[str]:
    """Deserialise a JSON string into ``List[str]``.

    Falls back to a single-element list wrapping the raw string if
    JSON parsing fails, guaranteeing that RAGAS always receives a
    non-empty list of strings.

    Parameters
    ----------
    raw : Any
        The value from the ``retrieved_contexts`` column – typically a
        JSON-encoded string, but may already be a Python list when the
        DataFrame has not been round-tripped through CSV.

    Returns
    -------
    List[str]
        A list of context strings suitable for RAGAS evaluation.
    """
    if isinstance(raw, list):
        return [str(item) for item in raw]

    if not isinstance(raw, str) or not raw.strip():
        logger.warning(
            "Empty or non-string context value encountered; "
            "returning placeholder."
        )
        return ["[NO CONTEXT RETRIEVED]"]

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        # If JSON decodes to a non-list (e.g. a dict), wrap it
        return [str(parsed)]
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning(
            "Failed to parse retrieved_contexts as JSON (%s); "
            "wrapping raw string as single-element list.",
            exc,
        )
        return [raw]


# =============================================================================
# Main evaluator class
# =============================================================================

class RagasEvaluator:
    """Scores RAG pipeline outputs using the RAGAS evaluation framework.

    Wraps :func:`ragas.evaluate` with column mapping, robust JSON
    deserialisation, and a thesis-ready summary aggregator so that a
    single call to :meth:`evaluate_dataframe` produces a DataFrame
    augmented with the four canonical RAGAS metrics.

    Parameters
    ----------
    evaluator_llm : Any
        A LangChain-compatible LLM instance used by RAGAS as the
        *judge* model (e.g. ``ChatOpenAI(model="gpt-4o")``).
    evaluator_embeddings : Any
        A LangChain-compatible embeddings instance used by RAGAS for
        semantic-similarity metrics (e.g.
        ``OpenAIEmbeddings(model="text-embedding-3-small")``).

    Examples
    --------
    >>> from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    >>> llm = ChatOpenAI(model="gpt-4o", temperature=0)
    >>> emb = OpenAIEmbeddings(model="text-embedding-3-small")
    >>> evaluator = RagasEvaluator(evaluator_llm=llm, evaluator_embeddings=emb)
    """

    def __init__(
        self,
        evaluator_llm: Any,
        evaluator_embeddings: Any,
    ) -> None:
        self._llm: Any = evaluator_llm
        self._embeddings: Any = evaluator_embeddings

        logger.info(
            "RagasEvaluator initialised  |  llm=%s  embeddings=%s",
            type(self._llm).__name__,
            type(self._embeddings).__name__,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score every row in *df* with the four RAGAS metrics.

        Uses a **row-by-row** evaluation strategy with inter-row
        ``time.sleep()`` delays and per-row exponential back-off
        retries to guard against HTTP 429 rate-limit responses from
        the HuggingFace serverless inference API.

        Parameters
        ----------
        df : pd.DataFrame
            The output of
            :meth:`~src.evaluation.runner.EvaluationRunner.export_to_pandas`.
            Must contain at minimum the columns ``question``,
            ``generated_answer``, ``retrieved_contexts``, and
            ``ground_truth``.

        Returns
        -------
        pd.DataFrame
            A copy of the original DataFrame augmented with four new
            float columns: ``faithfulness``, ``answer_relevancy``,
            ``context_precision``, and ``context_recall``.

        Raises
        ------
        ValueError
            If any required source column is missing from *df*.
        """
        logger.info(
            "Starting RAGAS evaluation  |  rows=%d  columns=%s",
            len(df),
            list(df.columns),
        )

        # -- 1. Validate required columns ---------------------------------
        missing: List[str] = [
            col for col in _COLUMN_MAP if col not in df.columns
        ]
        if missing:
            raise ValueError(
                f"Input DataFrame is missing required columns: {missing}. "
                f"Expected columns mapped from EvaluationRunner: "
                f"{list(_COLUMN_MAP.keys())}"
            )

        # -- 2. Initialise metric columns with NaN -----------------------
        result_df: pd.DataFrame = df.copy()
        metric_names: List[str] = [m.name for m in _RAGAS_METRICS]
        for col in metric_names:
            result_df[col] = float("nan")

        # -- 3. Evaluate row-by-row with rate-limit protection ------------
        total_rows: int = len(df)
        for idx in range(total_rows):
            row = df.iloc[idx]
            row_id: str = (
                f"{row.get('query_id', '?')}/{row.get('pipeline_type', '?')}"
            )

            logger.info(
                "Scoring row %d/%d  |  %s", idx + 1, total_rows, row_id
            )

            row_scores: Dict[str, float] = (
                self._evaluate_single_row_with_retries(
                    question=str(row["question"]),
                    answer=str(row["generated_answer"]),
                    contexts=_safe_parse_contexts(row["retrieved_contexts"]),
                    ground_truth=str(row["ground_truth"]),
                    row_id=row_id,
                )
            )

            # Write scores into the result DataFrame
            for col_name, score_value in row_scores.items():
                if col_name in result_df.columns:
                    result_df.at[result_df.index[idx], col_name] = score_value

            # Rate-limit delay between rows (skip after the last row)
            if idx < total_rows - 1:
                logger.debug(
                    "Sleeping %.1fs between RAGAS evaluations …",
                    _INTER_ROW_DELAY_SECONDS,
                )
                time.sleep(_INTER_ROW_DELAY_SECONDS)

        scored_count: int = result_df[metric_names[0]].notna().sum()
        logger.info(
            "RAGAS evaluation complete  |  %d/%d rows scored successfully.",
            scored_count,
            total_rows,
        )

        return result_df

    def generate_thesis_summary(
        self, evaluated_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate RAGAS metrics by pipeline type and hop depth.

        Produces a concise summary table that directly supports the
        thesis hypothesis: *Vector RAG degrades at higher hop depths
        while GraphRAG remains stable*.

        Parameters
        ----------
        evaluated_df : pd.DataFrame
            The output of :meth:`evaluate_dataframe` — the original
            runner DataFrame augmented with RAGAS metric columns.

        Returns
        -------
        pd.DataFrame
            A summary DataFrame grouped by ``['pipeline_type',
            'hop_depth']`` with mean values for ``latency_seconds``,
            ``faithfulness``, ``answer_relevancy``,
            ``context_precision``, and ``context_recall``.

        Raises
        ------
        ValueError
            If required grouping or metric columns are missing.
        """
        logger.info("Generating thesis summary from evaluated DataFrame.")

        # -- Validate required columns ------------------------------------
        required_cols: List[str] = [
            "pipeline_type",
            "hop_depth",
            *_SUMMARY_METRIC_COLS,
        ]
        missing: List[str] = [
            col for col in required_cols if col not in evaluated_df.columns
        ]
        if missing:
            raise ValueError(
                f"Evaluated DataFrame is missing columns required for "
                f"thesis summary: {missing}. Did you run "
                f"evaluate_dataframe() first?"
            )

        # -- Aggregate by pipeline × hop depth ----------------------------
        summary_df: pd.DataFrame = (
            evaluated_df
            .groupby(["pipeline_type", "hop_depth"], as_index=False)[
                _SUMMARY_METRIC_COLS
            ]
            .mean()
            .round(4)
            .sort_values(["pipeline_type", "hop_depth"])
            .reset_index(drop=True)
        )

        logger.info(
            "Thesis summary generated  |  shape=%s\n%s",
            summary_df.shape,
            summary_df.to_string(index=False),
        )

        return summary_df

    # # ------------------------------------------------------------------
    # # Private helpers (rate-limit protection)
    # # ------------------------------------------------------------------

    # def _evaluate_single_row_with_retries(
    #     self,
    #     question: str,
    #     answer: str,
    #     contexts: List[str],
    #     ground_truth: str,
    #     row_id: str,
    # ) -> Dict[str, float]:
    #     """Score a single row, retrying with exponential back-off on failure.

    #     Parameters
    #     ----------
    #     question, answer, contexts, ground_truth
    #         The four RAGAS-required fields for one evaluation row.
    #     row_id : str
    #         Human-readable identifier for logging (e.g. ``"TC-001/GraphRAG"``).

    #     Returns
    #     -------
    #     Dict[str, float]
    #         Metric name → score mapping.  Returns an empty dict if all
    #         retry attempts are exhausted (metrics stay ``NaN``).
    #     """
    #     for attempt in range(1, _MAX_ROW_RETRIES + 1):
    #         try:
    #             return self._evaluate_single_row(
    #                 question=question,
    #                 answer=answer,
    #                 contexts=contexts,
    #                 ground_truth=ground_truth,
    #             )
    #         except Exception as exc:
    #             backoff: float = _RETRY_BASE_DELAY_SECONDS * math.pow(
    #                 2, attempt - 1
    #             )
    #             logger.warning(
    #                 "RAGAS scoring failed for %s (attempt %d/%d): %s  "
    #                 "— retrying in %.1fs …",
    #                 row_id,
    #                 attempt,
    #                 _MAX_ROW_RETRIES,
    #                 exc,
    #                 backoff,
    #             )
    #             if attempt < _MAX_ROW_RETRIES:
    #                 time.sleep(backoff)

    #     logger.error(
    #         "All %d RAGAS retry attempts exhausted for %s; "
    #         "metrics will remain NaN.",
    #         _MAX_ROW_RETRIES,
    #         row_id,
    #     )
    #     return {}

    # def _evaluate_single_row(
    #     self,
    #     question: str,
    #     answer: str,
    #     contexts: List[str],
    #     ground_truth: str,
    # ) -> Dict[str, float]:
    #     """Run ``ragas.evaluate()`` on a single-row Dataset."""
    #     single_row_df = pd.DataFrame(
    #         {
    #             "question": [question],
    #             "answer": [answer],
    #             "contexts": [contexts],
    #             "ground_truth": [ground_truth],
    #         }
    #     )
    #     hf_dataset: Dataset = Dataset.from_pandas(single_row_df)

    #     ragas_result = evaluate(
    #         dataset=hf_dataset,
    #         metrics=_RAGAS_METRICS,
    #         llm=self._llm,
    #         embeddings=self._embeddings,
    #     )

    #     scores_df: pd.DataFrame = ragas_result.to_pandas()

    #     # Extract ONLY the requested metric values safely
    #     metric_scores: Dict[str, float] = {}
    #     expected_metrics = {m.name for m in _RAGAS_METRICS}
        
    #     for col in scores_df.columns:
    #         if col in expected_metrics:
    #             metric_scores[col] = float(scores_df[col].iloc[0])

    #     return metric_scores

    # # ------------------------------------------------------------------
    # # Dunder helpers
    # # ------------------------------------------------------------------

    # def __repr__(self) -> str:  # noqa: D105
    #     return (
    #         f"RagasEvaluator("
    #         f"llm={type(self._llm).__name__!r}, "
    #         f"embeddings={type(self._embeddings).__name__!r})"
    #     )

    # ------------------------------------------------------------------
    # Private helpers (rate-limit protection disabled)
    # ------------------------------------------------------------------

    def _evaluate_single_row_with_retries(
        self,
        question: str,
        answer: str,
        contexts: List[str],
        ground_truth: str,
        row_id: str,
    ) -> Dict[str, float]:
        """Score a single row. (Retry logic temporarily disabled for token conservation).

        Parameters
        ----------
        question, answer, contexts, ground_truth
            The four RAGAS-required fields for one evaluation row.
        row_id : str
            Human-readable identifier for logging (e.g. ``"TC-001/GraphRAG"``).

        Returns
        -------
        Dict[str, float]
            Metric name → score mapping. Returns an empty dict on failure.
        """
        try:
            return self._evaluate_single_row(
                question=question,
                answer=answer,
                contexts=contexts,
                ground_truth=ground_truth,
            )
        except Exception as exc:
            logger.error(
                "RAGAS scoring failed for %s: %s. "
                "[Retry logic disabled] Metrics will remain NaN.",
                row_id,
                exc,
            )
            return {}

    def _evaluate_single_row(
        self,
        question: str,
        answer: str,
        contexts: List[str],
        ground_truth: str,
    ) -> Dict[str, float]:
        """Run ``ragas.evaluate()`` on a single-row Dataset.

        Returns
        -------
        Dict[str, float]
            Metric name → score mapping for the single row.
        """
        single_row_df = pd.DataFrame(
            {
                "question": [question],
                "answer": [answer],
                "contexts": [contexts],
                "ground_truth": [ground_truth],
            }
        )
        hf_dataset: Dataset = Dataset.from_pandas(single_row_df)

        ragas_result = evaluate(
            dataset=hf_dataset,
            metrics=_RAGAS_METRICS,
            llm=self._llm,
            embeddings=self._embeddings,
        )

        scores_df: pd.DataFrame = ragas_result.to_pandas()  # type: ignore

        # Extract ONLY the requested metric values safely (Whitelist approach)
        metric_scores: Dict[str, float] = {}
        expected_metrics = {m.name for m in _RAGAS_METRICS}
        
        for col in scores_df.columns:
            if col in expected_metrics:
                metric_scores[col] = float(scores_df[col].iloc[0])

        return metric_scores

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"RagasEvaluator("
            f"llm={type(self._llm).__name__!r}, "
            f"embeddings={type(self._embeddings).__name__!r})"
        )
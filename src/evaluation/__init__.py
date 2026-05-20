"""
src.evaluation - Evaluation & Benchmarking for Vector RAG vs GraphRAG
======================================================================
Provides the ground-truth test-case schema, curated seed data, and the
evaluation runner used by the RAGAS evaluation framework to measure
*faithfulness*, *answer_relevancy*, *context_precision*, and
*context_recall*.
"""

from src.evaluation.runner import EvaluationResult, EvaluationRunner
from src.evaluation.schema import EvaluationTestCase
from src.evaluation.seed_data import get_thesis_test_cases
from src.evaluation.metrics_calculator import RagasEvaluator

__all__: list[str] = [
    "EvaluationResult",
    "EvaluationRunner",
    "EvaluationTestCase",
    "get_thesis_test_cases",
]

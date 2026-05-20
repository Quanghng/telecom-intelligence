"""
schema.py – Evaluation Test-Case Schema
=========================================
Pydantic model that defines the ground-truth structure consumed by the
RAGAS evaluation framework.

Each :class:`EvaluationTestCase` captures a single natural-language
question, the expected correct answer, the graph traversal depth
required to derive that answer, and a human-readable category tag.

Usage
-----
    >>> from src.evaluation.schema import EvaluationTestCase
    >>> tc = EvaluationTestCase(
    ...     query_id="TC-001",
    ...     question="Which physical Core_Routers have status 'Down'?",
    ...     ground_truth="Core_Router CR-01 (node_id 1) is currently Down.",
    ...     hop_depth=1,
    ...     category="Status Lookup",
    ... )
"""

from __future__ import annotations

import logging
from typing import Final, List

from pydantic import BaseModel, Field, field_validator

logger: logging.Logger = logging.getLogger(__name__)

# -- Valid category tags ------------------------------------------------------
_VALID_CATEGORIES: Final[List[str]] = [
    "Status Lookup",
    "Root Cause Analysis",
    "Blast Radius",
    "Capacity Check",
    "SLA Impact",
    "Dependency Mapping",
    "Path Analysis",
]


class EvaluationTestCase(BaseModel):
    """Ground-truth test case for RAGAS evaluation of RAG pipelines.

    Each instance represents a single evaluation record consisting of a
    natural-language *question*, a pre-verified *ground_truth* answer,
    the minimum number of relationship hops (*hop_depth*) a graph
    traversal needs to derive the answer, and a descriptive *category*.

    Attributes
    ----------
    query_id : str
        Unique identifier for the test case (e.g. ``"TC-001"``).
    question : str
        Natural-language question posed to the RAG system.
    ground_truth : str
        The correct, expected answer derived from the known graph
        topology.  Used as the reference for RAGAS ``faithfulness``
        and ``context_recall`` metrics.
    hop_depth : int
        Minimum number of graph edges that must be traversed to
        answer the question.  ``1`` = single-hop lookup,
        ``3`` = cross-layer traversal from physical → business.
    category : str
        Semantic category of the question.  Must be one of:
        ``"Status Lookup"``, ``"Root Cause Analysis"``,
        ``"Blast Radius"``, ``"Capacity Check"``,
        ``"SLA Impact"``, ``"Dependency Mapping"``,
        ``"Path Analysis"``.

    Examples
    --------
    >>> tc = EvaluationTestCase(
    ...     query_id="TC-003",
    ...     question="If Core_Router CR-01 goes Down, which Gold-SLA customers are affected?",
    ...     ground_truth="Enterprise_Corp_0 (Gold SLA) is affected via ...",
    ...     hop_depth=3,
    ...     category="Blast Radius",
    ... )
    >>> tc.hop_depth
    3
    """

    query_id: str = Field(
        ...,
        min_length=1,
        description="Unique test-case identifier (e.g. 'TC-001').",
    )
    question: str = Field(
        ...,
        min_length=10,
        description="Natural-language question posed to the RAG system.",
    )
    ground_truth: str = Field(
        ...,
        min_length=10,
        description=(
            "Pre-verified correct answer derived from the known graph "
            "topology, used as reference for RAGAS metrics."
        ),
    )
    hop_depth: int = Field(
        ...,
        ge=1,
        le=5,
        description=(
            "Minimum relationship hops required to answer the question. "
            "1 = single-hop lookup, 3 = physical→logical→service→business."
        ),
    )
    category: str = Field(
        ...,
        description=(
            "Semantic category of the question, e.g. 'Blast Radius', "
            "'Root Cause Analysis'."
        ),
    )

    # -- Validators -----------------------------------------------------------

    @field_validator("category")
    @classmethod
    def _category_must_be_known(cls, v: str) -> str:
        """Ensure *category* is one of the pre-defined evaluation tags."""
        if v not in _VALID_CATEGORIES:
            raise ValueError(
                f"Unknown category '{v}'. "
                f"Must be one of: {_VALID_CATEGORIES}"
            )
        return v

    @field_validator("question")
    @classmethod
    def _question_must_end_with_mark(cls, v: str) -> str:
        """Ensure the question ends with a question mark."""
        if not v.strip().endswith("?"):
            raise ValueError(
                "question must end with a '?' character. "
                f"Got: '…{v[-20:]}'"
            )
        return v

    # -- Convenience ----------------------------------------------------------

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"EvaluationTestCase(id={self.query_id!r}, "
            f"hops={self.hop_depth}, cat={self.category!r})"
        )

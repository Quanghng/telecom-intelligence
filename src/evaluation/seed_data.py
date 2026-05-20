"""
seed_data.py – Ground-Truth Seed Data for RAGAS Evaluation
============================================================
Provides a curated catalogue of evaluation test cases that exercise
the full four-layer telecom ontology:

    Physical → Logical → Service → Business

Each test case is designed to **expose the structural limitations of
Vector RAG** (cosine-similarity over flat text chunks) while
**highlighting the traversal strengths of GraphRAG** (Cypher-driven
multi-hop reasoning over Neo4j).

Design rationale
----------------
- **1-hop query** (``hop_depth=1``): Simple property look-up that
  *both* pipelines should answer correctly.  Serves as a calibration
  baseline proving parity between the two approaches.
- **2-hop query** (``hop_depth=2``): Logical-to-physical dependency
  look-up that begins to stretch Vector RAG.
- **3-hop query** (``hop_depth=3``): Cross-layer blast-radius analysis
  from physical through logical to service layer.  Vector RAG struggles.
- **4-hop query** (``hop_depth=4``): End-to-end path analysis chaining
  ``CONNECTS_TO`` + ``RUNS_ON`` + ``DEPENDS_ON`` + ``SUBSCRIBED_TO``.
  Near-impossible for Vector RAG without structured graph traversal.
  Serves as the decisive differentiator proving GraphRAG's superiority.

An optional ``target_hop`` parameter allows callers to filter by a
specific hop depth, enabling selective evaluation runs.

Usage
-----
    >>> from src.evaluation.seed_data import get_thesis_test_cases
    >>> cases = get_thesis_test_cases()
    >>> len(cases)
    4
    >>> [tc.hop_depth for tc in cases]
    [1, 2, 3, 4]
    >>> get_thesis_test_cases(target_hop=2)[0].hop_depth
    2
"""

from __future__ import annotations

import logging
from typing import List

from src.evaluation.schema import EvaluationTestCase

logger: logging.Logger = logging.getLogger(__name__)


def get_thesis_test_cases(target_hop: int | None = None) -> List[EvaluationTestCase]:
    """Return the curated ground-truth test cases for the thesis evaluation.

    The list contains test cases at hop depths 1, 2, 3, and 4 — ordered
    by ascending ``hop_depth``.  When *target_hop* is provided, only the
    cases matching that hop depth are returned.

    Parameters
    ----------
    target_hop : int | None, optional
        If provided, filter and return only test cases whose
        ``hop_depth`` equals this value.  ``None`` (the default)
        returns all test cases.

    Returns
    -------
    List[EvaluationTestCase]
        Validated test cases spanning the requested hop depths.

    Raises
    ------
    ValueError
        If *target_hop* is provided but no test cases match the
        requested hop depth.

    Examples
    --------
    >>> cases = get_thesis_test_cases()
    >>> [tc.hop_depth for tc in cases]
    [1, 2, 3, 4]
    >>> get_thesis_test_cases(target_hop=3)[0].category
    'Blast Radius'
    """
    cases: List[EvaluationTestCase] = [
        # ==================================================================
        # 1-HOP — Baseline calibration (Vector RAG should pass)
        # ==================================================================
        EvaluationTestCase(
            query_id="TC-001",
            question=(
                "Which physical-layer TelecomNodes with node_type "
                "'Core_Router' currently have status 'Down'?"
            ),
            ground_truth=(
                "The physical-layer Core_Router nodes with status 'Down' "
                "can be identified by: "
                "MATCH (n:TelecomNode:physical) "
                "WHERE n.node_type = 'Core_Router' AND n.status = 'Down' "
                "RETURN n.name, n.node_id, n.status. "
                "All Core_Router nodes whose status property equals 'Down' "
                "are returned."
            ),
            hop_depth=1,
            category="Status Lookup",
        ),
        # ==================================================================
        # 2-HOP — Logical-to-Physical dependency (stretches Vector RAG)
        # ==================================================================
        EvaluationTestCase(
            query_id="TC-004",
            question=(
                "Which logical-layer VirtualRouters run on a physical-layer "
                "Edge_Router that currently has status 'Degraded'?"
            ),
            ground_truth=(
                "2-hop traversal from logical to physical: "
                "MATCH (l:TelecomNode:logical)-[:RUNS_ON]->"
                "(p:TelecomNode:physical) "
                "WHERE p.node_type = 'Edge_Router' AND p.status = 'Degraded' "
                "RETURN l.name, l.node_id, p.name, p.status. "
                "This identifies every logical VirtualRouter hosted on a "
                "degraded Edge_Router."
            ),
            hop_depth=2,
            category="Dependency Mapping",
        ),
        # ==================================================================
        # 3-HOP — Cross-layer blast radius (Vector RAG struggles)
        # ==================================================================
        EvaluationTestCase(
            query_id="TC-006",
            question=(
                "If a physical-layer Core_Router with status 'Down' has "
                "logical-layer nodes running on it, which service-layer "
                "nodes depend on those logical nodes?"
            ),
            ground_truth=(
                "3-hop path from physical through logical to service: "
                "MATCH (p:TelecomNode:physical)<-[:RUNS_ON]-"
                "(l:TelecomNode:logical)<-[:DEPENDS_ON]-"
                "(s:TelecomNode:service) "
                "WHERE p.node_type = 'Core_Router' AND p.status = 'Down' "
                "RETURN p.name, l.name, s.name, s.service_type. "
                "This reveals the service-layer blast radius of a failed "
                "Core_Router propagating through its hosted logical nodes."
            ),
            hop_depth=3,
            category="Blast Radius",
        ),
        # ==================================================================
        # 4-HOP — End-to-end Path Analysis (Vector RAG cannot answer)
        # ==================================================================
        EvaluationTestCase(
            query_id="TC-009",
            question=(
                "Is there a path from a physical-layer Core_Router with "
                "status 'Down' to another physical-layer Edge_Router via "
                "CONNECTS_TO, and do any business-layer customers depend "
                "on that Edge_Router through the RUNS_ON, DEPENDS_ON, "
                "and SUBSCRIBED_TO chain?"
            ),
            ground_truth=(
                "4-hop path: physical CONNECTS_TO physical, then up to "
                "business: "
                "MATCH (cr:TelecomNode:physical)-[:CONNECTS_TO]-"
                "(er:TelecomNode:physical)<-[:RUNS_ON]-"
                "(l:TelecomNode:logical)<-[:DEPENDS_ON]-"
                "(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-"
                "(c:TelecomNode:business) "
                "WHERE cr.node_type = 'Core_Router' AND cr.status = 'Down' "
                "AND er.node_type = 'Edge_Router' "
                "RETURN cr.name, er.name, l.name, s.name, c.name, c.sla. "
                "This reveals the full downstream impact of a failed "
                "Core_Router propagating through an adjacent Edge_Router "
                "up to the business layer."
            ),
            hop_depth=4,
            category="Path Analysis",
        ),
    ]

    # -- Optional filtering ---------------------------------------------------
    if target_hop is not None:
        cases = [tc for tc in cases if tc.hop_depth == target_hop]
        if not cases:
            raise ValueError(
                f"No test cases found for target_hop={target_hop}. "
                f"Available hop depths: 1, 2, 3, 4."
            )

    logger.info(
        "Loaded %d ground-truth test cases  |  hop distribution: %s",
        len(cases),
        {tc.query_id: tc.hop_depth for tc in cases},
    )

    return cases
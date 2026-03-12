"""
ontology.py – OntologyMapper
==============================
Maps raw topology nodes and edges onto a four-layer telecom ontology:

    Physical  → Routers, switches, fibre/microwave links
    Logical   → VLANs, VPNs, MPLS tunnels
    Service   → VoLTE, IPTV, 5G network slices
    Business  → SLAs, customers, revenue streams

Each layer introduces its own node types, relationship semantics, and
attribute schemas that will later be ingested into Neo4j (Phase 2).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List

import networkx as nx


class OntologyLayer(Enum):
    """Enumeration of the four telecom ontology layers."""

    PHYSICAL = "physical"
    LOGICAL = "logical"
    SERVICE = "service"
    BUSINESS = "business"


class OntologyMapper:
    """Enriches a raw NetworkX graph with ontology-layer semantics.

    Parameters
    ----------
    config : Dict[str, Any]
        Parsed settings dictionary (expects ``data_generation.ontology``).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        ontology_cfg = config.get("data_generation", {}).get("ontology", {})
        self.layers: List[str] = ontology_cfg.get(
            "layers", [layer.value for layer in OntologyLayer]
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def map(self, graph: nx.Graph) -> nx.Graph:
        """Apply ontology-layer labels and attributes to every node/edge.

        Parameters
        ----------
        graph : nx.Graph
            The raw topology graph produced by ``TopologyBuilder``.

        Returns
        -------
        nx.Graph
            The same graph instance, enriched in-place with ontology
            attributes (``layer``, ``node_type``, ``properties``).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Layer-specific helpers
    # ------------------------------------------------------------------

    def _assign_physical_layer(self, graph: nx.Graph) -> None:
        """Assign Physical-layer roles (router, switch, antenna, etc.).

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        raise NotImplementedError

    def _assign_logical_layer(self, graph: nx.Graph) -> None:
        """Create Logical-layer overlay nodes (VLANs, VPNs, tunnels).

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        raise NotImplementedError

    def _assign_service_layer(self, graph: nx.Graph) -> None:
        """Attach Service-layer entities (VoLTE, IPTV, 5G slices).

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        raise NotImplementedError

    def _assign_business_layer(self, graph: nx.Graph) -> None:
        """Attach Business-layer entities (SLAs, customers, revenue).

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        raise NotImplementedError

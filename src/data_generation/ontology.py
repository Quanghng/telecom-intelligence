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

import random
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
        self.seed: int = config.get("data_generation", {}).get("seed", 42)
        self._rng: random.Random = random.Random(self.seed)

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
        self._assign_physical_layer(graph)
        self._assign_logical_layer(graph)
        self._assign_service_layer(graph)
        self._assign_business_layer(graph)
        return graph

    # ------------------------------------------------------------------
    # Layer-specific helpers
    # ------------------------------------------------------------------

    def _assign_physical_layer(self, graph: nx.Graph) -> None:
        """Assign Physical-layer roles (router, switch, antenna, etc.).

        Distribution (approximate):
            - 10 % Core_Router
            - 60 % Edge_Router
            - 30 % Cell_Tower

        Every node receives ``status='up'``.  Every edge receives random
        ``capacity_gbps`` (1–100) and ``latency_ms`` (1–20) attributes.

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        role_weights: List[tuple[str, float]] = [
            ("Core_Router", 0.10),
            ("Edge_Router", 0.60),
            ("Cell_Tower", 0.30),
        ]
        roles, weights = zip(*role_weights)

        for node in list(graph.nodes):
            chosen: str = self._rng.choices(roles, weights=weights, k=1)[0]
            graph.nodes[node]["node_type"] = chosen
            graph.nodes[node]["status"] = "up"
            graph.nodes[node]["layer"] = OntologyLayer.PHYSICAL.value

        for u, v in graph.edges:
            graph.edges[u, v]["capacity_gbps"] = self._rng.randint(1, 100)
            graph.edges[u, v]["latency_ms"] = round(self._rng.uniform(1.0, 20.0), 2)
            graph.edges[u, v]["layer"] = OntologyLayer.PHYSICAL.value

    def _assign_logical_layer(self, graph: nx.Graph) -> None:
        """Create Logical-layer overlay nodes (VLANs, VPNs, tunnels).

        Adds synthetic VLAN and MPLS_Tunnel nodes and connects each to a
        randomly selected ``Edge_Router`` in the physical layer.

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        edge_routers: List[int] = [
            n for n, d in graph.nodes(data=True)
            if d.get("node_type") == "Edge_Router"
        ]
        if not edge_routers:
            return

        logical_types: List[str] = ["VLAN", "MPLS_Tunnel"]
        num_logical: int = max(1, len(edge_routers) // 5)
        next_id: int = max(graph.nodes) + 1

        for i in range(num_logical):
            node_id: int = next_id + i
            ltype: str = self._rng.choice(logical_types)
            graph.add_node(
                node_id,
                layer=OntologyLayer.LOGICAL.value,
                node_type=ltype,
                name=f"{ltype}_{i}",
                status="up",
            )
            target: int = self._rng.choice(edge_routers)
            graph.add_edge(
                node_id, target,
                layer=OntologyLayer.LOGICAL.value,
                relationship="RUNS_ON",
            )

    def _assign_service_layer(self, graph: nx.Graph) -> None:
        """Attach Service-layer entities (VoLTE, IPTV, 5G slices).

        Adds service nodes and connects each to a randomly selected
        logical-layer node.

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        logical_nodes: List[int] = [
            n for n, d in graph.nodes(data=True)
            if d.get("layer") == OntologyLayer.LOGICAL.value
        ]
        if not logical_nodes:
            return

        service_types: List[str] = ["VoLTE", "5G_Slice", "Enterprise_VPN", "IPTV"]
        num_services: int = max(1, len(logical_nodes) // 2)
        next_id: int = max(graph.nodes) + 1

        for i in range(num_services):
            node_id: int = next_id + i
            stype: str = self._rng.choice(service_types)
            graph.add_node(
                node_id,
                layer=OntologyLayer.SERVICE.value,
                node_type=stype,
                name=f"{stype}_{i}",
                status="active",
            )
            target: int = self._rng.choice(logical_nodes)
            graph.add_edge(
                node_id, target,
                layer=OntologyLayer.SERVICE.value,
                relationship="DEPENDS_ON",
            )

    def _assign_business_layer(self, graph: nx.Graph) -> None:
        """Attach Business-layer entities (SLAs, customers, revenue).

        Adds customer nodes with an ``sla`` attribute (Gold / Silver /
        Bronze) and connects each to a randomly selected service node.

        Parameters
        ----------
        graph : nx.Graph
            Graph to mutate in-place.
        """
        service_nodes: List[int] = [
            n for n, d in graph.nodes(data=True)
            if d.get("layer") == OntologyLayer.SERVICE.value
        ]
        if not service_nodes:
            return

        customer_names: List[str] = [
            "Enterprise_Corp", "Consumer_Base", "Govt_Agency",
            "SMB_Partner", "Wholesale_Carrier",
        ]
        sla_tiers: List[str] = ["Gold", "Silver", "Bronze"]
        num_customers: int = max(1, len(service_nodes))
        next_id: int = max(graph.nodes) + 1

        for i in range(num_customers):
            node_id: int = next_id + i
            cname: str = self._rng.choice(customer_names)
            sla: str = self._rng.choice(sla_tiers)
            graph.add_node(
                node_id,
                layer=OntologyLayer.BUSINESS.value,
                node_type="Customer",
                name=f"{cname}_{i}",
                sla=sla,
                status="active",
            )
            target: int = self._rng.choice(service_nodes)
            graph.add_edge(
                node_id, target,
                layer=OntologyLayer.BUSINESS.value,
                relationship="SUBSCRIBED_TO",
            )

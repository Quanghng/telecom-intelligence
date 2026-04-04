"""
chaos.py – ChaosMonkey
=======================
Stochastic fault-injection engine for synthetic telecom topologies.

Supported fault types
---------------------
- **link_down**      – Marks an edge as failed (``status='down'``).
- **node_overload**  – Sets CPU/memory utilisation above threshold.
- **latency_spike**  – Inflates edge latency by a random multiplier.
- **packet_loss**    – Assigns a packet-loss percentage to an edge.

Faults are injected probabilistically according to
``data_generation.chaos.fault_probability`` in ``settings.yaml``.
"""

from __future__ import annotations

import random
from typing import Any, Dict, List

import networkx as nx


class ChaosMonkey:
    """Injects realistic faults into an enriched telecom graph.

    Parameters
    ----------
    config : Dict[str, Any]
        Parsed settings dictionary (expects ``data_generation.chaos``).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        chaos_cfg = config.get("data_generation", {}).get("chaos", {})
        self.fault_probability: float = chaos_cfg.get("fault_probability", 0.05)
        self.fault_types: List[str] = chaos_cfg.get("fault_types", [])
        self.seed: int = config.get("data_generation", {}).get("seed", 42)
        self._rng: random.Random = random.Random(self.seed)
        self.fault_report: Dict[str, int] = {ft: 0 for ft in self.fault_types}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def inject(self, graph: nx.Graph) -> nx.Graph:
        """Apply random faults to the graph.

        Iterates over edges (and optionally nodes) and, with probability
        ``fault_probability``, applies one of the configured fault types.

        Parameters
        ----------
        graph : nx.Graph
            An ontology-enriched graph.

        Returns
        -------
        nx.Graph
            The same graph instance with fault attributes injected.
        """
        if not self.fault_types:
            return graph

        edge_fault_map: Dict[str, Any] = {
            "link_down": self._inject_link_down,
            "latency_spike": self._inject_latency_spike,
            "packet_loss": self._inject_packet_loss,
        }
        node_fault_map: Dict[str, Any] = {
            "node_overload": self._inject_node_overload,
            "node_down": self._inject_node_down,            
            "node_degraded": self._inject_node_degraded,
        }

        edge_fault_types: List[str] = [
            ft for ft in self.fault_types if ft in edge_fault_map
        ]
        node_fault_types: List[str] = [
            ft for ft in self.fault_types if ft in node_fault_map
        ]

        # Inject faults on physical-layer edges
        for u, v, data in graph.edges(data=True):
            if data.get("layer") != "physical":
                continue
            if self._rng.random() < self.fault_probability and edge_fault_types:
                fault: str = self._rng.choice(edge_fault_types)
                edge_fault_map[fault](graph, u, v)

        # Inject faults on physical-layer nodes
        for node, data in graph.nodes(data=True):
            if data.get("layer") != "physical":
                continue
            if self._rng.random() < self.fault_probability and node_fault_types:
                fault = self._rng.choice(node_fault_types)
                node_fault_map[fault](graph, node)

        return graph

    def get_fault_report(self, graph: nx.Graph) -> Dict[str, Any]:
        """Summarise all injected faults.

        Parameters
        ----------
        graph : nx.Graph
            Graph that has already been through ``inject()``.

        Returns
        -------
        Dict[str, Any]
            Counts and details per fault type.
        """
        return self.fault_report

    # ------------------------------------------------------------------
    # Fault-type handlers
    # ------------------------------------------------------------------

    def _inject_link_down(self, graph: nx.Graph, u: int, v: int) -> None:
        """Mark edge (u, v) as down.

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        u, v : int
            Edge endpoints.
        """
        graph.edges[u, v]["status"] = "down"
        self.fault_report["link_down"] += 1

    def _inject_node_overload(self, graph: nx.Graph, node: int) -> None:
        """Simulate CPU / memory overload on *node*.

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        node : int
            Node identifier.
        """
        graph.nodes[node]["cpu_utilization"] = round(self._rng.uniform(95.0, 100.0), 2)
        self.fault_report["node_overload"] += 1
    def _inject_node_down(self, graph: nx.Graph, node: int) -> None:
        """Simulate a complete node failure."""
        graph.nodes[node]["status"] = "Down"
        if "node_down" not in self.fault_report:
            self.fault_report["node_down"] = 0
        self.fault_report["node_down"] += 1

    def _inject_node_degraded(self, graph: nx.Graph, node: int) -> None:
        """Simulate a degraded node state."""
        graph.nodes[node]["status"] = "Degraded"
        if "node_degraded" not in self.fault_report:
            self.fault_report["node_degraded"] = 0
        self.fault_report["node_degraded"] += 1

    def _inject_latency_spike(self, graph: nx.Graph, u: int, v: int) -> None:
        """Inflate latency on edge (u, v).

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        u, v : int
            Edge endpoints.
        """
        current: float = graph.edges[u, v].get("latency_ms", 1.0)
        graph.edges[u, v]["latency_ms"] = round(current * self._rng.uniform(5.0, 10.0), 2)
        self.fault_report["latency_spike"] += 1

    def _inject_packet_loss(self, graph: nx.Graph, u: int, v: int) -> None:
        """Assign packet-loss percentage to edge (u, v).

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        u, v : int
            Edge endpoints.
        """
        graph.edges[u, v]["packet_loss_pct"] = round(self._rng.uniform(5.0, 20.0), 2)
        self.fault_report["packet_loss"] += 1

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def _inject_node_overload(self, graph: nx.Graph, node: int) -> None:
        """Simulate CPU / memory overload on *node*.

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        node : int
            Node identifier.
        """
        raise NotImplementedError

    def _inject_latency_spike(self, graph: nx.Graph, u: int, v: int) -> None:
        """Inflate latency on edge (u, v).

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        u, v : int
            Edge endpoints.
        """
        raise NotImplementedError

    def _inject_packet_loss(self, graph: nx.Graph, u: int, v: int) -> None:
        """Assign packet-loss percentage to edge (u, v).

        Parameters
        ----------
        graph : nx.Graph
            Target graph.
        u, v : int
            Edge endpoints.
        """
        raise NotImplementedError

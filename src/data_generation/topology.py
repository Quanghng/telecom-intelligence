"""
topology.py – TopologyBuilder
==============================
Wraps NetworkX to construct a Barabási–Albert scale-free graph that
models a realistic telecom backbone topology.

The resulting graph is an ``nx.Graph`` where each node and edge carries
attribute dictionaries that downstream stages (OntologyMapper, ChaosMonkey)
will enrich.
"""

from __future__ import annotations

from typing import Any, Dict

import networkx as nx


class TopologyBuilder:
    """Builds a scale-free network topology using Barabási–Albert model.

    Parameters
    ----------
    config : Dict[str, Any]
        Parsed settings dictionary (expects ``data_generation.topology``).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        topo_cfg = config.get("data_generation", {}).get("topology", {})
        self.num_nodes: int = topo_cfg.get("num_nodes", 500)
        self.attachment_edges: int = topo_cfg.get("attachment_edges", 3)
        self.seed: int = config.get("data_generation", {}).get("seed", 42)
        self.graph: nx.Graph | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self) -> nx.Graph:
        """Generate the Barabási–Albert graph.

        Returns
        -------
        nx.Graph
            The constructed scale-free graph with raw (un-enriched) nodes.
        """
        raise NotImplementedError

    def get_graph(self) -> nx.Graph:
        """Return the most recently built graph.

        Returns
        -------
        nx.Graph
            The current graph instance.

        Raises
        ------
        RuntimeError
            If ``build()`` has not been called yet.
        """
        if self.graph is None:
            raise RuntimeError("Graph not built yet. Call build() first.")
        return self.graph

    def summary(self) -> Dict[str, Any]:
        """Return basic graph statistics.

        Returns
        -------
        Dict[str, Any]
            Keys: ``num_nodes``, ``num_edges``, ``avg_degree``,
            ``density``, ``is_connected``.
        """
        raise NotImplementedError

"""
exporter.py – DataExporter
===========================
Serialises an enriched NetworkX graph to flat files (CSV, JSON) ready
for Neo4j bulk import (Phase 2) or standalone analysis.

Output structure (inside ``data/raw/``)::

    nodes.csv          – One row per node with all attributes.
    edges.csv          – One row per edge with all attributes.
    graph.json         – Full graph in node-link JSON format.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import networkx as nx


class DataExporter:
    """Exports a NetworkX graph to CSV and/or JSON.

    Parameters
    ----------
    config : Dict[str, Any]
        Parsed settings dictionary (expects ``data_generation.export``).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        export_cfg = config.get("data_generation", {}).get("export", {})
        self.formats: List[str] = export_cfg.get("formats", ["csv", "json"])
        self.output_dir: Path = Path(export_cfg.get("output_dir", "data/raw"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(self, graph: nx.Graph) -> List[Path]:
        """Export the graph in all configured formats.

        Parameters
        ----------
        graph : nx.Graph
            The fully enriched (ontology + faults) graph.

        Returns
        -------
        List[Path]
            Paths to the files that were written.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Format-specific writers
    # ------------------------------------------------------------------

    def _export_csv(self, graph: nx.Graph) -> List[Path]:
        """Write ``nodes.csv`` and ``edges.csv``.

        Parameters
        ----------
        graph : nx.Graph
            Source graph.

        Returns
        -------
        List[Path]
            Paths to the two CSV files.
        """
        raise NotImplementedError

    def _export_json(self, graph: nx.Graph) -> Path:
        """Write ``graph.json`` in node-link format.

        Parameters
        ----------
        graph : nx.Graph
            Source graph.

        Returns
        -------
        Path
            Path to the JSON file.
        """
        raise NotImplementedError

    def _ensure_output_dir(self) -> None:
        """Create the output directory if it does not exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

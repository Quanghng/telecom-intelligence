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

import csv
import json
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
        self._ensure_output_dir()
        paths: List[Path] = []

        if "csv" in self.formats:
            paths.extend(self._export_csv(graph))
        if "json" in self.formats:
            paths.append(self._export_json(graph))

        return paths

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
        # -- nodes.csv ------------------------------------------------
        nodes_path: Path = self.output_dir / "nodes.csv"
        node_rows: List[Dict[str, Any]] = []
        for node_id, attrs in graph.nodes(data=True):
            row: Dict[str, Any] = {"node_id": node_id}
            for key, value in attrs.items():
                row[key] = json.dumps(value) if isinstance(value, (dict, list)) else value
            node_rows.append(row)

        if node_rows:
            fieldnames: List[str] = ["node_id"] + [
                k for k in node_rows[0] if k != "node_id"
            ]
            # Collect any extra keys that appear in later rows
            all_keys: set[str] = set()
            for row in node_rows:
                all_keys.update(row.keys())
            for k in all_keys:
                if k not in fieldnames:
                    fieldnames.append(k)

            with open(nodes_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(node_rows)

        # -- edges.csv ------------------------------------------------
        edges_path: Path = self.output_dir / "edges.csv"
        edge_rows: List[Dict[str, Any]] = []
        for u, v, attrs in graph.edges(data=True):
            row = {"source": u, "target": v}
            for key, value in attrs.items():
                row[key] = json.dumps(value) if isinstance(value, (dict, list)) else value
            edge_rows.append(row)

        if edge_rows:
            fieldnames = ["source", "target"] + [
                k for k in edge_rows[0] if k not in ("source", "target")
            ]
            all_keys = set()
            for row in edge_rows:
                all_keys.update(row.keys())
            for k in all_keys:
                if k not in fieldnames:
                    fieldnames.append(k)

            with open(edges_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(edge_rows)

        return [nodes_path, edges_path]

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
        json_path: Path = self.output_dir / "graph.json"
        data: Dict[str, Any] = dict(nx.node_link_data(graph, link="links"))
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
        return json_path

    def _ensure_output_dir(self) -> None:
        """Create the output directory if it does not exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

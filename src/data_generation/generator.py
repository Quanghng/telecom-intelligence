"""
generator.py – SandboxOrchestrator
===================================
End-to-end orchestrator for synthetic telecom data generation.

Coordinates the full Phase 1 pipeline:
    TopologyBuilder → OntologyMapper → ChaosMonkey → DataExporter

Usage
-----
    >>> from src.data_generation import SandboxOrchestrator
    >>> orch = SandboxOrchestrator("config/settings.yaml")
    >>> orch.run()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from .topology import TopologyBuilder
from .ontology import OntologyMapper
from .chaos import ChaosMonkey
from .exporter import DataExporter


class SandboxOrchestrator:
    """Orchestrates the full synthetic-data generation pipeline.

    Parameters
    ----------
    config_path : str | Path
        Path to the YAML configuration file (``config/settings.yaml``).
    """

    def __init__(self, config_path: str | Path) -> None:
        self.config: Dict[str, Any] = self._load_config(config_path)
        self.topology_builder = TopologyBuilder(self.config)
        self.ontology_mapper = OntologyMapper(self.config)
        self.chaos_monkey = ChaosMonkey(self.config)
        self.data_exporter = DataExporter(self.config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the complete generation pipeline.

        Steps
        -----
        1. Build a scale-free topology (Barabási–Albert).
        2. Map nodes/edges onto the four-layer telecom ontology.
        3. Inject stochastic faults.
        4. Export the enriched graph to CSV / JSON.
        """
        # 1. Build topology
        graph = self.topology_builder.build()

        # 2. Enrich with ontology layers
        graph = self.ontology_mapper.map(graph)

        # 3. Inject faults
        graph = self.chaos_monkey.inject(graph)

        # 4. Export to CSV / JSON
        export_paths = self.data_exporter.export(graph)

        # 5. Print summary
        summary = self.topology_builder.summary()
        fault_report = self.chaos_monkey.get_fault_report(graph)

        print("=" * 60)
        print("  Telecom Sandbox – Generation Complete")
        print("=" * 60)
        print(f"  Nodes          : {summary['num_nodes']}")
        print(f"  Edges          : {summary['num_edges']}")
        print(f"  Avg Degree     : {summary['avg_degree']:.2f}")
        print(f"  Density        : {summary['density']:.6f}")
        print(f"  Connected      : {summary['is_connected']}")
        print("-" * 60)
        print("  Fault Report:")
        for fault_type, count in fault_report.items():
            print(f"    {fault_type:<20s}: {count}")
        print("-" * 60)
        print("  Exported Files:")
        for path in export_paths:
            print(f"    → {path}")
        print("=" * 60)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(config_path: str | Path) -> Dict[str, Any]:
        """Read and parse the YAML configuration file.

        Parameters
        ----------
        config_path : str | Path
            Filesystem path to ``settings.yaml``.

        Returns
        -------
        Dict[str, Any]
            Parsed configuration dictionary.
        """
        with open(config_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)

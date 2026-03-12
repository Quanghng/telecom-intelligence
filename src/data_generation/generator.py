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
        raise NotImplementedError

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

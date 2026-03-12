"""
data_generation
===============
Phase 1 – Synthetic telecom network data generator.

Uses a Barabási–Albert preferential-attachment model (NetworkX) to produce
a realistic scale-free topology, maps it onto a four-layer telecom ontology,
injects faults via ChaosMonkey, and exports the result as CSV / JSON.

Public API
----------
SandboxOrchestrator : End-to-end pipeline orchestrator.
TopologyBuilder     : NetworkX graph construction.
OntologyMapper      : Layer assignment (Physical → Business).
ChaosMonkey         : Stochastic fault injection.
DataExporter        : Serialisation to CSV / JSON.
"""

from .generator import SandboxOrchestrator
from .topology import TopologyBuilder
from .ontology import OntologyMapper
from .chaos import ChaosMonkey
from .exporter import DataExporter

__all__ = [
    "SandboxOrchestrator",
    "TopologyBuilder",
    "OntologyMapper",
    "ChaosMonkey",
    "DataExporter",
]

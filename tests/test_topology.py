"""
test_topology.py – Unit tests for TopologyBuilder
===================================================
Validates graph construction, statistics, and deterministic seeding.
"""

from __future__ import annotations

import pytest
import networkx as nx

from src.data_generation.topology import TopologyBuilder


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def default_config() -> dict:
    """Minimal config dict matching settings.yaml defaults."""
    return {
        "data_generation": {
            "seed": 42,
            "topology": {
                "model": "barabasi_albert",
                "num_nodes": 50,
                "attachment_edges": 2,
            },
        }
    }


@pytest.fixture
def builder(default_config: dict) -> TopologyBuilder:
    """A TopologyBuilder initialised with the default test config."""
    return TopologyBuilder(default_config)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

class TestTopologyBuilderInit:
    """Constructor and configuration parsing."""

    def test_reads_num_nodes(self, builder: TopologyBuilder) -> None:
        """num_nodes should come from config."""
        assert builder.num_nodes == 50

    def test_reads_attachment_edges(self, builder: TopologyBuilder) -> None:
        """attachment_edges should come from config."""
        assert builder.attachment_edges == 2

    def test_reads_seed(self, builder: TopologyBuilder) -> None:
        """Seed should come from the top-level data_generation key."""
        assert builder.seed == 42

    def test_graph_is_none_before_build(self, builder: TopologyBuilder) -> None:
        """Graph should be None until build() is called."""
        assert builder.graph is None


class TestTopologyBuilderBuild:
    """Graph construction (will pass once build() is implemented)."""

    def test_build_returns_graph(self, builder: TopologyBuilder) -> None:
        graph = builder.build()
        assert isinstance(graph, nx.Graph)

    def test_build_node_count(self, builder: TopologyBuilder) -> None:
        graph = builder.build()
        assert graph.number_of_nodes() == builder.num_nodes

    def test_build_is_deterministic(self, default_config: dict) -> None:
        """Two builders with the same seed should produce identical graphs."""
        g1 = TopologyBuilder(default_config).build()
        g2 = TopologyBuilder(default_config).build()
        assert nx.utils.graphs_equal(g1, g2)


class TestTopologyBuilderGetGraph:
    """get_graph() guard behaviour."""

    def test_raises_before_build(self, builder: TopologyBuilder) -> None:
        with pytest.raises(RuntimeError, match="not built yet"):
            builder.get_graph()


class TestTopologyBuilderSummary:
    """summary() output shape (will pass once implemented)."""

    def test_summary_keys(self, builder: TopologyBuilder) -> None:
        builder.build()
        stats = builder.summary()
        expected_keys = {"num_nodes", "num_edges", "avg_degree", "density", "is_connected"}
        assert expected_keys.issubset(stats.keys())

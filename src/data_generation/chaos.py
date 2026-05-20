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
import logging
import random
from typing import Any, Dict, List, Optional

import networkx as nx

logger = logging.getLogger(__name__)

class ChaosMonkey:
    """Injects realistic faults into an enriched telecom graph."""

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
        """Apply random faults and deterministic ground-truths to the graph."""
        
        if self.fault_types:
            graph = self._apply_stochastic_chaos(graph)

        # CRITICAL: Enforce thesis benchmarks after random chaos to prevent overwriting
        graph = self.enforce_ground_truths(graph)
        
        return graph

    def enforce_ground_truths(self, graph: nx.Graph) -> nx.Graph:
        """
        Ensures specific topological conditions exist for thesis benchmarking.
        """
        logger.info("Enforcing deterministic ground-truth topologies for all hops...")
        self._seed_tc_004(graph) # Hop-2 (logical -> physical)
        self._seed_tc_006(graph) # Hop-3 (service -> logical -> physical)
        self._seed_hop_4(graph)  # Hop-4 (business -> service -> logical -> physical)
        return graph

    def get_fault_report(self, graph: nx.Graph) -> Dict[str, Any]:
        """Summarise all injected faults."""
        return self.fault_report

    # ------------------------------------------------------------------
    # Deterministic Seeding (Thesis Test Cases)
    # ------------------------------------------------------------------

    def _seed_tc_004(self, graph: nx.Graph) -> None:
        """
        Ensures TC-004: logical-layer VirtualRouter runs on a 
        physical-layer Edge_Router with status 'Degraded'.
        """
        target_physical_node: Optional[Any] = None
        
        # 1. Search for an existing valid topology path
        for u, v, data in graph.edges(data=True):
            # networkx is technically undirected by default unless DiGraph, handle both directions
            node_u = graph.nodes[u]
            node_v = graph.nodes[v]
            
            # Match strict schema: (VirtualRouter)-[:RUNS_ON]->(Edge_Router)
            if node_u.get("node_type") == "VirtualRouter" and node_v.get("node_type") == "Edge_Router":
                target_physical_node = v
                break
            elif node_v.get("node_type") == "VirtualRouter" and node_u.get("node_type") == "Edge_Router":
                target_physical_node = u
                break

        # 2. Mutate state or inject missing topology
        if target_physical_node is not None:
            graph.nodes[target_physical_node]["status"] = "Degraded"
            logger.info(f"TC-004 Seed: Mutated existing Edge_Router '{target_physical_node}' to 'Degraded'.")
        else:
            # Inject synthetic nodes if the base generator failed to create this dependency
            logger.warning("TC-004 topology not found. Injecting synthetic subgraph.")
            p_node_id = "SYNTH-EDGE-001"
            l_node_id = "SYNTH-VR-001"
            
            graph.add_node(p_node_id, layer="physical", node_type="Edge_Router", status="Degraded", 
                           name="Synth-Edge-Router", sla="Silver", cpu_utilization=95.0)
            graph.add_node(l_node_id, layer="logical", node_type="VirtualRouter", status="up", 
                           name="Synth-Virtual-Router", sla="Gold", cpu_utilization=40.0)
            
            # Add strict uppercase relationship
            graph.add_edge(l_node_id, p_node_id, type="RUNS_ON")

    def _seed_tc_006(self, graph: nx.Graph) -> None:
        """
        Ensures Hop-3 Ground Truth: 
        (service)-[:DEPENDS_ON]->(logical)-[:RUNS_ON]->(physical Core_Router 'Down').
        """
        logger.info("Injecting TC-006 3-hop synthetic topology.")
        
        p_node_id = "SYNTH-CORE-001"
        l_node_id = "SYNTH-LOGICAL-002"
        s_node_id = "SYNTH-SERVICE-001"
        
        # 1. Inject Nodes
        graph.add_node(p_node_id, layer="physical", node_type="Core_Router", status="Down", 
                       name="Synth-Core-Router-Failed", sla="Silver", cpu_utilization=0.0)
        graph.add_node(l_node_id, layer="logical", node_type="MPLS_Tunnel", status="Degraded", 
                       name="Synth-MPLS-Backbone", sla="Gold", cpu_utilization=0.0)
        graph.add_node(s_node_id, layer="service", node_type="Enterprise_VPN", status="Degraded", 
                       name="Synth-B2B-VPN", sla="Gold", cpu_utilization=0.0)
        
        # 2. Inject Strict Directed Edges
        graph.add_edge(l_node_id, p_node_id, type="RUNS_ON")
        graph.add_edge(s_node_id, l_node_id, type="DEPENDS_ON")

    def _seed_hop_4(self, graph: nx.Graph) -> None:
        """
        Ensures Hop-4 Ground Truth for TC-009: 
        (Core_Router 'Down')-[:CONNECTS_TO]-(Edge_Router)<-[:RUNS_ON]-(logical)<-[:DEPENDS_ON]-(service)<-[:SUBSCRIBED_TO]-(business Customer).
        """
        logger.info("Injecting 4-hop synthetic topology with horizontal physical path.")
        
        c_node_id = "SYNTH-H4-CORE-999"
        e_node_id = "SYNTH-H4-EDGE-555"
        l_node_id = "SYNTH-H4-VLAN-888"
        s_node_id = "SYNTH-H4-IPTV-777"
        b_node_id = "SYNTH-H4-CUST-666"
        
        # 1. Inject Nodes
        graph.add_node(c_node_id, layer="physical", node_type="Core_Router", status="Down", 
                       name="H4-Failed-Core", sla="Gold", cpu_utilization=0.0)
        graph.add_node(e_node_id, layer="physical", node_type="Edge_Router", status="up", 
                       name="H4-Active-Edge", sla="Gold", cpu_utilization=45.0)
        graph.add_node(l_node_id, layer="logical", node_type="VLAN", status="Degraded", 
                       name="H4-VLAN-Segment", sla="Gold", cpu_utilization=0.0)
        graph.add_node(s_node_id, layer="service", node_type="IPTV", status="Degraded", 
                       name="H4-IPTV-Broadcast", sla="Gold", cpu_utilization=0.0)
        graph.add_node(b_node_id, layer="business", node_type="Customer", status="Degraded", 
                       name="H4-Enterprise-Client", sla="Gold", cpu_utilization=0.0)
        
        # 2. Inject Strict Directed Edges matching TC-009
        graph.add_edge(c_node_id, e_node_id, type="CONNECTS_TO") # Horizontal physical hop
        graph.add_edge(l_node_id, e_node_id, type="RUNS_ON")     # Logical runs on EDGE, not CORE
        graph.add_edge(s_node_id, l_node_id, type="DEPENDS_ON")
        graph.add_edge(b_node_id, s_node_id, type="SUBSCRIBED_TO")

    # ------------------------------------------------------------------
    # Fault-type handlers (Stochastic)
    # ------------------------------------------------------------------

    def _apply_stochastic_chaos(self, graph: nx.Graph) -> nx.Graph:
        """Original probabilistic injection logic."""
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

        edge_fault_types: List[str] = [ft for ft in self.fault_types if ft in edge_fault_map]
        node_fault_types: List[str] = [ft for ft in self.fault_types if ft in node_fault_map]

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

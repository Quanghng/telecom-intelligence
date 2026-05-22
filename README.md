# Telecom Operational Intelligence — GraphRAG for Optical Network Fault Analysis

[![Build Status](https://img.shields.io/github/actions/workflow/status/YOUR_USERNAME/telecom-intelligence/ci.yml?branch=main&style=flat-square)](https://github.com/YOUR_USERNAME/telecom-intelligence/actions)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.x-008CC1?style=flat-square&logo=neo4j&logoColor=white)](https://neo4j.com/)

---

## Overview

**telecom-intelligence** is a research-grade pipeline that demonstrates how **GraphRAG** (Neo4j + LangGraph + Cypher generation) decisively outperforms standard **Vector RAG** (ChromaDB + cosine similarity) when reasoning over multi-hop telecom network topologies. The system generates a synthetic optical-network digital twin, ingests it into a property graph, and uses an LLM-powered state machine with self-correcting Cypher feedback loops to compute fault **blast radii** — from a single fibre cut all the way up to impacted Gold-SLA customers.

This work accompanies the thesis *"Intelligence Opérationnelle dans les Télécoms : Utilisation du GraphRAG pour corréler la topologie des réseaux optiques et l'impact client en temps réel"*. The core claim — validated empirically with RAGAS metrics across 1-to-4-hop queries — is that flat embedding retrieval induces **structural hallucinations** the moment relational depth exceeds one hop, while graph traversal maintains perfect context precision and recall regardless of path length.

<!-- Replace with an actual screenshot or animated demo -->
<p align="center">
  <img src="docs/assets/demo.gif" alt="GraphRAG Blast Radius Demo" width="720" />
</p>

---

## Features

- **Synthetic Topology Generator** — Barabási–Albert scale-free graph with 4-layer telecom ontology (Physical → Logical → Service → Business) and configurable chaos injection (link down, node overload, latency spikes, packet loss).
- **Neo4j ETL Pipeline** — Batch-streamed CSV ingestion with automated constraint/index DDL and idempotent `MERGE` semantics.
- **LangGraph State Machine Orchestrator** — Cyclical directed graph with dynamic context assembly, few-shot Cypher vector memory, and up to 3 self-correction iterations on syntax errors or empty results.
- **Cypher Vector Memory** — ChromaDB-backed example store for few-shot prompt retrieval using `text-embedding-3-small`.
- **Baseline Vector RAG** — Full ChromaDB-based retrieval arm (sentence-transformers embeddings) for controlled A/B comparison.
- **RAGAS Evaluation Suite** — Automated benchmarking across faithfulness, answer relevancy, context precision, and context recall at each hop depth.
- **Deterministic BFS Oracle** — Ground-truth blast-radius computation via breadth-first search for evaluation scoring.
- **Fully Containerised** — Single `docker compose up` for Neo4j with persistent volumes.

---

## Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16 GB |
| Disk | 5 GB free | 10 GB (Neo4j data + ChromaDB) |
| GPU | Not required | — |
| OS | Windows 10+, Ubuntu 20.04+, macOS 12+ | Any x86_64/arm64 with Docker |

> **Note:** The LLM inference is offloaded to external APIs (HuggingFace Inference / OpenAI). No local GPU is needed. Neo4j page-cache is capped at 1 GB by default in `docker-compose.yml`.

---

## Installation

### Prerequisites

- **Python 3.11+**
- **Docker & Docker Compose** (for Neo4j)
- API keys:
  - `HF_TOKEN` — [HuggingFace](https://huggingface.co/settings/tokens) (Qwen2.5-7B-Instruct via serverless inference)
  - `OPENAI_API_KEY` — [OpenAI](https://platform.openai.com/api-keys) (embeddings: `text-embedding-3-small`)

### Step-by-step

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/telecom-intelligence.git
cd telecom-intelligence

# 2. Create and activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env and set HF_TOKEN and OPENAI_API_KEY

# 5. Start Neo4j
docker compose up -d
# Verify at http://localhost:7474 (neo4j / changeme)
```

---

## Getting Started / Quickstart

### Phase 1 — Generate Synthetic Topology

```bash
python -m src.data_generation
```

Outputs `data/raw/nodes.csv` and `data/raw/edges.csv` (500-node Barabási–Albert graph with fault injection).

### Phase 2 — Ingest into Neo4j

```bash
python -m src.pipeline
```

### Phase 3 — Populate Cypher Vector Memory

```bash
python populate_vector_db.py
```

### Phase 4 — Run a GraphRAG Query

```bash
python run_graphrag.py
```

### Phase 5 — Run the Full A/B Evaluation

```bash
python run_evaluation.py
```

Results are written to `data/scored_evaluation_results.csv`.

---

## Project Structure

```
telecom-intelligence/
├── config/
│   ├── settings.py            # Pydantic BaseSettings (env + .env)
│   └── settings.yaml          # Master YAML configuration
├── src/
│   ├── pipeline.py            # Neo4j ETL orchestrator (Phase 2)
│   ├── baseline/
│   │   ├── vector_etl.py      # ChromaDB ingestion for baseline
│   │   └── vector_retriever.py# Cosine-similarity retrieval arm
│   ├── core/
│   │   └── orchestrator.py    # LangGraph StateGraph (Phase 3)
│   ├── data_generation/
│   │   ├── chaos.py           # Fault injection engine
│   │   ├── exporter.py        # CSV/JSON export
│   │   ├── generator.py       # SandboxOrchestrator entry point
│   │   ├── ontology.py        # 4-layer telecom ontology
│   │   └── topology.py        # Barabási–Albert graph builder
│   ├── database/
│   │   ├── connection.py      # Neo4j driver context manager
│   │   ├── sanitizer.py       # Cypher injection prevention
│   │   └── schema.py          # Constraint & index DDL
│   ├── evaluation/
│   │   ├── metrics_calculator.py  # RAGAS metric scoring
│   │   ├── runner.py          # A/B evaluation engine
│   │   ├── schema.py          # EvaluationTestCase model
│   │   └── seed_data.py       # Ground-truth test cases (1–4 hops)
│   ├── ingestion/
│   │   ├── extractor.py       # Batched CSV streaming
│   │   └── loader.py          # Neo4j MERGE batch writer
│   └── llm/
│       ├── graph_orchestrator.py  # Prompt assembly & Cypher gen
│       ├── summary_agent.py   # Natural-language synthesis
│       └── vector_memory.py   # ChromaDB few-shot Cypher store
├── data/
│   ├── raw/                   # Generated CSVs (nodes, edges)
│   ├── chroma_db/             # Baseline vector store
│   ├── cypher_vector_db/      # Few-shot Cypher examples
│   └── neo4j_data/            # Persistent Neo4j volume
├── tests/
├── docker-compose.yml         # Neo4j container definition
├── requirements.txt
├── run_evaluation.py          # Phase 5: full A/B benchmark CLI
├── run_graphrag.py            # Phase 3: integration test script
├── plot_metrics.py            # Thesis figure generation
└── populate_vector_db.py      # Phase 3: seed Cypher vector memory
```

---

## Evaluation & Metrics

The evaluation suite runs 10 curated test cases (spanning 1-hop to 4-hop traversals) against both pipelines and computes [RAGAS](https://docs.ragas.io/) metrics:

| Metric | GraphRAG (avg) | VectorRAG (avg) | Δ |
|--------|:--------------:|:---------------:|:-:|
| **Faithfulness** | 0.986 | 0.807 | +22% |
| **Answer Relevancy** | 0.777 | 0.611 | +27% |
| **Context Precision** | 1.000 | 0.125 | +700% |
| **Context Recall** | 1.000 | 0.125 | +700% |

> **Key finding:** Vector RAG context precision/recall collapses to **0.0** at hop depths ≥ 3, while GraphRAG maintains **1.0** across all depths — confirming the structural hallucination hypothesis.

| Hop Depth | GraphRAG Latency (s) | VectorRAG Latency (s) |
|:---------:|:--------------------:|:---------------------:|
| 1 | 1.97 | 0.65 |
| 2 | 1.24 | 0.77 |
| 3 | 2.14 | 0.71 |
| 4 | 2.85 | 1.06 |

GraphRAG trades ~1–2 s additional latency for **correct** multi-hop answers.

To regenerate evaluation figures:

```bash
python plot_metrics.py
```

---

## Configuration

All pipeline parameters are centralised in `config/settings.yaml`:

```yaml
data_generation:
  topology:
    model: "barabasi_albert"
    num_nodes: 500
    attachment_edges: 3
  chaos:
    fault_probability: 0.05
    fault_types: [link_down, node_overload, latency_spike, packet_loss, node_down, node_degraded]

neo4j:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "changeme"

agent:
  llm:
    provider: "openai"
    model: "gpt-4o"
    temperature: 0.0
  cypher:
    max_retries: 3
    self_correct: true
```

Runtime secrets are loaded from `.env` via `pydantic-settings`.

---

## License & Citation

This project is released under the **MIT License**. See [LICENSE](LICENSE) for details.
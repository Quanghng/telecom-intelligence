"""
vector_memory.py – Dynamic Vector Routing via ChromaDB
=======================================================
Implements **Pillar 1 (Dynamic Vector Routing)** and **Pillar 2
(Dynamic Context Assembly)** of the Dynamic GraphRAG architecture.

Architecture Overview
---------------------
Traditional GraphRAG systems inject the entire graph schema and a
fixed set of few-shot examples into every LLM prompt.  This is
wasteful and degrades generation quality as schema complexity grows.

This module replaces that static approach with a **semantic retrieval
layer** backed by a persistent ChromaDB vector collection:

1. A curated *Cypher Query Library* stores canonical question→Cypher
   pairs alongside the **partial schema** (specific layers, node types,
   and relationships) required by each query.

2. At inference time, the user's natural-language prompt is embedded
   with OpenAI's ``text-embedding-3-small`` model and used to query
   ChromaDB for the *k=3* most semantically relevant examples.

3. The **Schema Union** of those *k* retrieved examples is computed
   as the mathematical set union of their required nodes,
   relationships, and properties — yielding a minimal, query-specific
   schema context rather than injecting the full ontology.

This design minimises prompt token consumption while maximising
contextual relevance, following the Dynamic Few-Shot RAG paradigm
(Lewis et al., 2020; Gao et al., 2023).

Components
----------
- :data:`CYPHER_QUERY_LIBRARY` – The structured few-shot knowledge base.
- :class:`CypherVectorMemory` – ChromaDB population, embedding, and
  retrieval logic with schema union computation.

Usage
-----
    >>> from src.llm.vector_memory import CypherVectorMemory
    >>> memory = CypherVectorMemory(openai_api_key="sk-...")
    >>> memory.populate()  # one-time indexing
    >>> context = memory.retrieve("Show all down routers", k=3)
    >>> print(context.few_shot_examples)
    >>> print(context.schema_union)
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Final, List, Set

import chromadb
from chromadb.api import ClientAPI
from chromadb.config import Settings as ChromaSettings
from openai import OpenAI

logger: logging.Logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Project paths
# ------------------------------------------------------------------

_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_DEFAULT_CHROMA_DIR: Final[Path] = _PROJECT_ROOT / "data" / "cypher_vector_db"
_COLLECTION_NAME: Final[str] = "cypher_few_shot_examples"

# ------------------------------------------------------------------
# Cypher Query Library – Structured Few-Shot Knowledge Base
# ------------------------------------------------------------------
# Each entry contains:
#   - question: the canonical natural-language question
#   - cypher: the gold-standard Cypher query
#   - partial_schema: the minimal set of layers, node_types, and
#     relationships exercised by this query
#
# The partial_schema enables Dynamic Context Assembly (Pillar 2):
# at retrieval time, the union of retrieved partial schemas produces
# the *minimal* schema context needed for generation.
# ------------------------------------------------------------------

CYPHER_QUERY_LIBRARY: Final[List[Dict[str, Any]]] = [
    {
        "id": "ex_01_physical_down_routers",
        "question": "Which physical-layer TelecomNodes with node_type 'Core_Router' currently have status 'Down'?",
        "cypher": (
            "MATCH (n:TelecomNode:physical {node_type: 'Core_Router'}) \n"
            "WHERE n.status = 'Down' \n"
            "RETURN n \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical"],
            "node_types": ["Core_Router"],
            "relationships": [],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_02_logical_on_degraded_physical",
        "question": "Which logical-layer VirtualRouters run on a physical-layer Edge_Router that currently has status 'Degraded'?",
        "cypher": (
            "MATCH (l:TelecomNode:logical {node_type: 'VirtualRouter'})-[:RUNS_ON]->(p:TelecomNode:physical {node_type: 'Edge_Router'}) \n"
            "WHERE p.status = 'Degraded' \n"
            "RETURN l, p \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["logical", "physical"],
            "node_types": ["VirtualRouter", "Edge_Router"],
            "relationships": ["RUNS_ON"],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_03_degraded_subnet_physical",
        "question": "A logical-layer Subnet has status 'Degraded'; which physical-layer Switch does it run on?",
        "cypher": (
            "MATCH (v:TelecomNode:logical {node_type: 'Subnet'})-[:RUNS_ON]->(r:TelecomNode:physical {node_type: 'Switch'})\n"
            "WHERE v.status = 'Degraded'\n"
            "RETURN v, r\n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["logical", "physical"],
            "node_types": ["Subnet", "Switch"],
            "relationships": ["RUNS_ON"],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_04_physical_to_service_3hop",
        "question": "If a physical-layer Core_Router with status 'Down' has logical-layer nodes running on it, which service-layer nodes depend on those logical nodes?",
        "cypher": (
            "MATCH (p:TelecomNode:physical {node_type: 'Core_Router'})<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service) \n"
            "WHERE p.status = 'Down' \n"
            "RETURN p, l, s \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical", "logical", "service"],
            "node_types": ["Core_Router"],
            "relationships": ["RUNS_ON", "DEPENDS_ON"],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_05_physical_to_business_full_chain",
        "question": "If physical-layer Edge_Router 'XYZ' goes 'Down', which business-layer customers are affected?",
        "cypher": (
            "MATCH (r:TelecomNode:physical {node_type: 'Edge_Router', name: 'XYZ'}) \n"
            "WHERE r.status = 'Down' \n"
            "OPTIONAL MATCH (r)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business) \n"
            "RETURN r, l, s, c \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical", "logical", "service", "business"],
            "node_types": ["Edge_Router"],
            "relationships": ["RUNS_ON", "DEPENDS_ON", "SUBSCRIBED_TO"],
            "properties_used": ["node_type", "name", "status"],
        },
    },
    {
        "id": "ex_06_connects_to_with_business_impact",
        "question": "Is there a path from a physical-layer Core_Router with status 'Down' to another physical-layer Edge_Router via CONNECTS_TO, and do any business-layer customers depend on that Edge_Router?",
        "cypher": (
            "MATCH (cr:TelecomNode:physical {node_type: 'Core_Router'})-[:CONNECTS_TO]-(er:TelecomNode:physical {node_type: 'Edge_Router'}) \n"
            "WHERE cr.status = 'Down' \n"
            "OPTIONAL MATCH (er)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business) \n"
            "RETURN cr, er, l, s, c \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical", "logical", "service", "business"],
            "node_types": ["Core_Router", "Edge_Router"],
            "relationships": ["CONNECTS_TO", "RUNS_ON", "DEPENDS_ON", "SUBSCRIBED_TO"],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_07_high_latency_silver_customers",
        "question": "For each CONNECTS_TO link with high latency, which business-layer Silver customers are reachable?",
        "cypher": (
            "MATCH (a:TelecomNode:physical)-[link:CONNECTS_TO]-(b:TelecomNode:physical)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(c:TelecomNode:business {sla: 'Silver'}) \n"
            "WHERE toFloat(split(link.latency_ms, 'ms')[0]) > 50 \n"
            "RETURN a, link, b, l, s, c \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical", "logical", "service", "business"],
            "node_types": [],
            "relationships": ["CONNECTS_TO", "RUNS_ON", "DEPENDS_ON", "SUBSCRIBED_TO"],
            "properties_used": ["sla", "latency_ms"],
        },
    },
    {
        "id": "ex_08_degraded_vlans",
        "question": "Which logical-layer VLANs with status 'Degraded' run on physical-layer nodes?",
        "cypher": (
            "MATCH (v:TelecomNode:logical {node_type: 'VLAN'})-[:RUNS_ON]->(r:TelecomNode:physical)\n"
            "WHERE v.status = 'Degraded'\n"
            "RETURN v, r\n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["logical", "physical"],
            "node_types": ["VLAN"],
            "relationships": ["RUNS_ON"],
            "properties_used": ["node_type", "status"],
        },
    },
    {
        "id": "ex_09_routers_down_degraded_gold_customers",
        "question": "Find any physical routers (Core_Router or Edge_Router) that are currently Down or Degraded. What logical layers and services depend on them, and are any Gold SLA customers impacted?",
        "cypher": (
            "MATCH (p:TelecomNode:physical) \n"
            "WHERE p.node_type IN ['Core_Router', 'Edge_Router'] AND p.status IN ['Down', 'Degraded'] \n"
            "OPTIONAL MATCH (p)<-[:RUNS_ON]-(l:TelecomNode:logical)<-[:DEPENDS_ON]-(s:TelecomNode:service)<-[:SUBSCRIBED_TO]-(b:TelecomNode:business) \n"
            "WHERE b.sla = 'Gold' \n"
            "RETURN p, l, s, b \n"
            "LIMIT 25"
        ),
        "partial_schema": {
            "layers": ["physical", "logical", "service", "business"],
            "node_types": ["Core_Router", "Edge_Router"],
            "relationships": ["RUNS_ON", "DEPENDS_ON", "SUBSCRIBED_TO"],
            "properties_used": ["node_type", "status", "sla"],
        },
    },
]


# ------------------------------------------------------------------
# Data class for retrieval results
# ------------------------------------------------------------------


@dataclass
class RetrievedContext:
    """Container for the dynamically assembled LLM context.

    Attributes
    ----------
    few_shot_examples : List[Dict[str, Any]]
        The top-k retrieved examples from the Cypher Query Library,
        each containing 'question', 'cypher', and 'partial_schema'.
    schema_union : Dict[str, Set[str]]
        The mathematical set union of all partial schemas across
        the retrieved examples.  Keys: 'layers', 'node_types',
        'relationships', 'properties_used'.
    distances : List[float]
        ChromaDB L2 distances for each retrieved example (lower is
        more relevant).
    """

    few_shot_examples: List[Dict[str, Any]] = field(default_factory=list)
    schema_union: Dict[str, Set[str]] = field(default_factory=dict)
    distances: List[float] = field(default_factory=list)

    def format_schema_block(self) -> str:
        """Render the schema union as a human-readable prompt block.

        Returns
        -------
        str
            A formatted string suitable for injection into the LLM
            system prompt's schema section.
        """
        layers = sorted(self.schema_union.get("layers", set()))
        node_types = sorted(self.schema_union.get("node_types", set()))
        relationships = sorted(self.schema_union.get("relationships", set()))
        properties = sorted(self.schema_union.get("properties_used", set()))

        lines = [
            "## DYNAMICALLY ASSEMBLED SCHEMA (union of retrieved examples)",
            f"Layers: {', '.join(layers) if layers else 'all'}",
            f"Node Types: {', '.join(node_types) if node_types else 'any TelecomNode'}",
            f"Relationships: {', '.join(relationships) if relationships else 'none specific'}",
            f"Properties: {', '.join(properties) if properties else 'standard'}",
        ]
        return "\n".join(lines)

    def format_examples_block(self) -> str:
        """Render retrieved few-shot examples for injection into the prompt.

        Returns
        -------
        str
            Formatted question/Cypher pairs for the LLM.
        """
        lines = ["## DYNAMICALLY RETRIEVED FEW-SHOT EXAMPLES"]
        for i, ex in enumerate(self.few_shot_examples, 1):
            lines.append(f"\n### Example {i}")
            lines.append(f"Question: {ex['question']}")
            lines.append(f"Cypher:\n{ex['cypher']}")
        return "\n".join(lines)


# ------------------------------------------------------------------
# CypherVectorMemory – ChromaDB management and retrieval
# ------------------------------------------------------------------


class CypherVectorMemory:
    """Persistent vector store for the Cypher Query Library.

    This class manages a ChromaDB collection that indexes the canonical
    few-shot examples by their natural-language questions.  At query
    time, it retrieves the *k* most semantically similar examples and
    computes their schema union for Dynamic Context Assembly.

    Parameters
    ----------
    openai_api_key : str
        API key for OpenAI (used for ``text-embedding-3-small``).
    chroma_dir : Path | str, optional
        Directory for persistent ChromaDB storage.
    collection_name : str, optional
        Name of the ChromaDB collection.
    embedding_model : str, optional
        OpenAI embedding model identifier.

    Notes
    -----
    The embedding function uses OpenAI's ``text-embedding-3-small``
    (1536-d, $0.02/1M tokens) for high-quality semantic similarity.
    This is intentionally decoupled from the generation model (which
    uses HuggingFace Serverless Inference) to leverage the superior
    retrieval quality of dedicated embedding models.
    """

    def __init__(
        self,
        openai_api_key: str,
        chroma_dir: Path | str = _DEFAULT_CHROMA_DIR,
        collection_name: str = _COLLECTION_NAME,
        embedding_model: str = "text-embedding-3-small",
    ) -> None:
        self._openai_client = OpenAI(api_key=openai_api_key)
        self._embedding_model: str = embedding_model
        self._chroma_dir: Path = Path(chroma_dir)
        self._collection_name: str = collection_name

        # Initialise persistent ChromaDB client
        self._chroma_client: ClientAPI = chromadb.PersistentClient(
            path=str(self._chroma_dir),
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        self._collection = self._chroma_client.get_or_create_collection(
            name=self._collection_name,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            "CypherVectorMemory initialised (dir=%s, collection=%s, "
            "existing_count=%d)",
            self._chroma_dir,
            self._collection_name,
            self._collection.count(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def populate(self, force: bool = False) -> int:
        """Index the Cypher Query Library into ChromaDB.

        Embeds each canonical question using ``text-embedding-3-small``
        and upserts it into the persistent collection.  Uses content-
        based hashing to avoid redundant re-embedding on subsequent
        calls.

        Parameters
        ----------
        force : bool
            If True, delete and recreate the collection before
            populating.

        Returns
        -------
        int
            Number of examples indexed.
        """
        if force:
            self._chroma_client.delete_collection(self._collection_name)
            self._collection = self._chroma_client.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info("Collection force-cleared.")

        # Check if already populated (idempotent)
        if self._collection.count() >= len(CYPHER_QUERY_LIBRARY) and not force:
            logger.info(
                "Collection already populated (%d items). Skipping.",
                self._collection.count(),
            )
            return self._collection.count()

        # Batch-embed all questions
        questions: List[str] = [ex["question"] for ex in CYPHER_QUERY_LIBRARY]
        embeddings: List[List[float]] = self._embed_texts(questions)

        # Prepare ChromaDB upsert payload
        ids: List[str] = [ex["id"] for ex in CYPHER_QUERY_LIBRARY]
        documents: List[str] = questions
        metadatas: List[Dict[str, Any]] = [
            {
                "cypher": ex["cypher"],
                "layers": ",".join(ex["partial_schema"]["layers"]),
                "node_types": ",".join(ex["partial_schema"]["node_types"]),
                "relationships": ",".join(ex["partial_schema"]["relationships"]),
                "properties_used": ",".join(ex["partial_schema"]["properties_used"]),
                "content_hash": hashlib.sha256(
                    (ex["question"] + ex["cypher"]).encode()
                ).hexdigest()[:16],
            }
            for ex in CYPHER_QUERY_LIBRARY
        ]

        self._collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )

        logger.info("Populated %d examples into ChromaDB.", len(ids))
        return len(ids)

    def retrieve(self, user_prompt: str, k: int = 3) -> RetrievedContext:
        """Retrieve the top-k most relevant examples for a user prompt.

        Implements Dynamic Vector Routing: the user's question is
        embedded and compared against the indexed Cypher Query Library.
        The retrieved examples' partial schemas are then unioned to
        produce the minimal schema context (Dynamic Context Assembly).

        Parameters
        ----------
        user_prompt : str
            The natural-language telecom question to route.
        k : int, optional
            Number of examples to retrieve (default 3).

        Returns
        -------
        RetrievedContext
            Contains the top-k examples, their schema union, and
            similarity distances.
        """
        # Ensure collection is populated
        if self._collection.count() == 0:
            logger.warning("Collection empty — populating on first retrieval.")
            self.populate()

        # Embed user prompt
        query_embedding: List[float] = self._embed_texts([user_prompt])[0]

        # Query ChromaDB
        results = self._collection.query(
            query_embeddings=[query_embedding],
            n_results=min(k, self._collection.count()),
            include=["documents", "metadatas", "distances"],
        )

        # Parse results into structured examples
        few_shot_examples: List[Dict[str, Any]] = []
        distances: List[float] = results["distances"][0] if results["distances"] else []

        for i, (doc, meta) in enumerate(
            zip(results["documents"][0], results["metadatas"][0])
        ):
            example: Dict[str, Any] = {
                "question": doc,
                "cypher": meta["cypher"],
                "partial_schema": {
                    "layers": [l for l in meta["layers"].split(",") if l],
                    "node_types": [n for n in meta["node_types"].split(",") if n],
                    "relationships": [r for r in meta["relationships"].split(",") if r],
                    "properties_used": [
                        p for p in meta["properties_used"].split(",") if p
                    ],
                },
            }
            few_shot_examples.append(example)

        # Compute schema union (Pillar 2: Dynamic Context Assembly)
        schema_union = self._compute_schema_union(few_shot_examples)

        context = RetrievedContext(
            few_shot_examples=few_shot_examples,
            schema_union=schema_union,
            distances=distances,
        )

        logger.info(
            "Retrieved %d examples (distances: %s). Schema union: layers=%s, "
            "rels=%s",
            len(few_shot_examples),
            [f"{d:.3f}" for d in distances],
            schema_union.get("layers", set()),
            schema_union.get("relationships", set()),
        )

        return context

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of texts using OpenAI's embedding API.

        Parameters
        ----------
        texts : List[str]
            Texts to embed.

        Returns
        -------
        List[List[float]]
            List of embedding vectors (1536-dimensional for
            text-embedding-3-small).
        """
        response = self._openai_client.embeddings.create(
            input=texts,
            model=self._embedding_model,
        )
        return [item.embedding for item in response.data]

    @staticmethod
    def _compute_schema_union(
        examples: List[Dict[str, Any]],
    ) -> Dict[str, Set[str]]:
        """Compute the mathematical set union of partial schemas.

        Given a list of retrieved few-shot examples, each with a
        ``partial_schema`` dictionary, this method computes the union
        across all schema dimensions (layers, node_types,
        relationships, properties_used).

        This implements the core of **Pillar 2: Dynamic Context
        Assembly** — only the schema elements actually relevant to the
        retrieved examples are injected into the generation prompt.

        Parameters
        ----------
        examples : List[Dict[str, Any]]
            Retrieved examples, each containing a 'partial_schema' key.

        Returns
        -------
        Dict[str, Set[str]]
            Union of all schema dimensions.
        """
        union: Dict[str, Set[str]] = {
            "layers": set(),
            "node_types": set(),
            "relationships": set(),
            "properties_used": set(),
        }

        for ex in examples:
            schema = ex.get("partial_schema", {})
            for key in union:
                union[key].update(schema.get(key, []))

        return union

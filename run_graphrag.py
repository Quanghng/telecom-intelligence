"""
Integration test script for the Phase 3 GraphRAG pipeline.
Tests the full flow: Query → Cypher Generation → Graph Retrieval → Synthesis Report.
Uses the LangGraph StateGraph orchestrator with Neo4j feedback loop.
"""

import os
import logging

from dotenv import load_dotenv
from openai import OpenAI

from src.database.connection import Neo4jConnection
from src.core.orchestrator import GraphRAGOrchestrator


def main():
    # ── Setup ────────────────────────────────────────────────────────────
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    load_dotenv()
    api_key = os.getenv("HF_TOKEN")
    if not api_key:
        raise ValueError(
            "HF_TOKEN is missing. Set it in your .env file or environment."
        )

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise ValueError(
            "OPENAI_API_KEY is missing. Set it in your .env file or environment."
        )

    client = OpenAI(
        base_url="https://router.huggingface.co/v1", 
        api_key=api_key
    )

    # ── Initialization ───────────────────────────────────────────────────
    hf_model = "Qwen/Qwen2.5-7B-Instruct"

    # ── Test Execution ───────────────────────────────────────────────────
    test_query = (
        'Find any physical routers that are currently "Down" or "Degraded". '
        "What logical layers and services depend on them, and are any "
        "Gold SLA customers impacted?"
    )

    logger.info("Starting GraphRAG pipeline integration test...")
    logger.info("Test query: %s", test_query)

    try:
        with Neo4jConnection() as conn:
            orchestrator = GraphRAGOrchestrator(
                connection=conn,
                llm_client=client,
                openai_api_key=openai_api_key,
                model=hf_model,
                temperature=0.0,
                k=3,
            )
            result = orchestrator.run_pipeline(test_query)

        # ── Output Processing ────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("  STAGE 1 — Generated Cypher Query")
        print("=" * 70)
        print(result.get("generated_cypher", "N/A"))

        print("\n" + "=" * 70)
        print("  STAGE 2 — Graph Data Retrieved")
        print("=" * 70)
        print(f"Records returned: {result.get('graph_data_length', 0)}")

        print("\n" + "=" * 70)
        print("  STAGE 3 — Final Operational Intelligence Report")
        print("=" * 70)
        print(result.get("final_report", "N/A"))
        print("=" * 70 + "\n")

        logger.info("Integration test completed successfully.")

    except Exception as exc:
        logger.error("Pipeline failure: %s", exc, exc_info=True)
        print(f"\n[ERROR] GraphRAG pipeline failed: {exc}")


if __name__ == "__main__":
    main()
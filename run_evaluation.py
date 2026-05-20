#!/usr/bin/env python
"""
run_evaluation.py – Telecom Operational Intelligence: A/B Evaluation CLI
=========================================================================
Main execution script for Phase 4, Step 5.  Drives the full evaluation
pipeline comparing **Vector RAG** (ChromaDB + cosine similarity) against
**GraphRAG** (Neo4j + Cypher traversal) across 10 curated ground-truth
test cases spanning 1-hop to 4-hop telecom ontology queries.

Workflow
--------
1. Initialise the LLM (HuggingFace serverless via ``ChatOpenAI``),
   embeddings (``HuggingFaceEmbeddings``), and all pipeline components.
2. Execute every test case against both RAG arms via
   :class:`~src.evaluation.runner.EvaluationRunner`.
3. Score the results with RAGAS metrics via
   :class:`~src.evaluation.metrics_calculator.RagasEvaluator`.
4. Persist the scored DataFrame to ``data/scored_evaluation_results.csv``
   and print a summary to the console.

Usage
-----
    $ python run_evaluation.py
"""

from __future__ import annotations

import os
import argparse
import logging
import sys
from pathlib import Path
from pydantic import SecretStr

import pandas as pd
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_openai import ChatOpenAI
from openai import OpenAI

from config.settings import llm_settings
from src.baseline.vector_retriever import VectorRetriever
from src.core.orchestrator import GraphRAGOrchestrator
from src.database.connection import Neo4jConnection
from src.evaluation.metrics_calculator import RagasEvaluator
from src.evaluation.runner import EvaluationRunner
from src.evaluation.seed_data import get_thesis_test_cases
from src.llm.cypher_agent import CypherGenerationAgent
from src.llm.summary_agent import SynthesisAgent

# -- Constants ----------------------------------------------------------------
_PROJECT_ROOT: Path = Path(__file__).resolve().parent
_LOG_FILE_PATH: Path = _PROJECT_ROOT / "logs" / "evaluation_debug.log"
_HF_BASE_URL: str = "https://api.groq.com/openai/v1"
_HF_MODEL: str = "llama-3.3-70b-versatile"
_EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"

logger: logging.Logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Set up root logger with a console handler and file handler."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Avoid duplicate handlers when re-running in an interactive session
    if root.handlers:
        return

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    # 2. File Handler (Ensure directory exists first)
    _LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Using mode="w" to overwrite the log file on each fresh run. 
    # Change to "a" if you want to append across multiple runs.
    file_handler = logging.FileHandler(_LOG_FILE_PATH, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Suppress known noisy third-party warnings
    logging.getLogger("ragas.prompt.pydantic_prompt").setLevel(logging.ERROR)
    logging.getLogger("huggingface_hub.utils._http").setLevel(logging.ERROR)


def main() -> None:
    """Execute the full Telecom Operational Intelligence evaluation pipeline."""
    _configure_logging()

    # ==================================================================
    # 0. CLI Argument Parsing
    # ==================================================================
    parser = argparse.ArgumentParser(
        description="Telecom Operational Intelligence – A/B Evaluation CLI",
    )
    parser.add_argument(
        "--hop",
        type=int,
        choices=[1, 2, 3, 4],
        required=True,
        help="Target hop depth to evaluate (1-4).",
    )
    parser.add_argument(
        "--token",
        type=str,
        required=True,
        help="HuggingFace API token for LLM inference.",
    )
    args = parser.parse_args()

    # -- Dynamic output paths based on hop depth --------------------------
    raw_csv_path: Path = _PROJECT_ROOT / "data" / f"evaluation_results_hop_{args.hop}.csv"
    scored_csv_path: Path = _PROJECT_ROOT / "data" / f"scored_evaluation_results_hop_{args.hop}.csv"

    logger.info("=" * 70)
    logger.info("TELECOM OPERATIONAL INTELLIGENCE – Evaluation Pipeline Starting")
    logger.info("=" * 70)
    logger.info("Target hop depth: %d", args.hop)
    logger.info("Raw CSV path: %s", raw_csv_path)
    logger.info("Scored CSV path: %s", scored_csv_path)

    # ==================================================================
    # 1. Initialisation
    # ==================================================================
    api_key: str = args.token
    api_key_secret = SecretStr(api_key)  # Wrap in SecretStr for safety
    # Safely inject HF_TOKEN into the environment for downstream libraries

    if llm_settings.hf_token:
        os.environ["HF_TOKEN"] = llm_settings.hf_token.get_secret_value()
        logger.debug("Hugging Face token securely injected into environment.")

    # -- LangChain-compatible LLM (judge model for RAGAS) ----------------
    logger.info("Initialising LangChain ChatOpenAI (HuggingFace serverless) …")
    evaluator_llm = ChatOpenAI(
        model=_HF_MODEL,
        api_key=api_key_secret,
        base_url=_HF_BASE_URL,
        temperature=0.0,
        max_retries=2,
        timeout=120,
    )

    # -- LangChain-compatible Embeddings (for RAGAS semantic metrics) ----
    logger.info("Initialising HuggingFaceEmbeddings (%s) …", _EMBEDDING_MODEL)
    evaluator_embeddings = HuggingFaceEmbeddings(
        model_name=_EMBEDDING_MODEL,
    )

    # -- Raw OpenAI client for pipeline agents (CypherAgent / Synthesis) -
    logger.info("Initialising OpenAI client for pipeline agents …")
    openai_client = OpenAI(
        base_url=_HF_BASE_URL,
        api_key=api_key,
    )

    cypher_agent = CypherGenerationAgent(client=openai_client, model=_HF_MODEL)
    synthesis_agent = SynthesisAgent(client=openai_client, model=_HF_MODEL)

    # -- Neo4j connection & GraphRAG orchestrator ------------------------
    logger.info("Connecting to Neo4j …")
    neo4j_conn = Neo4jConnection()
    neo4j_conn.__enter__()

    try:
      orchestrator = GraphRAGOrchestrator(
          connection=neo4j_conn,
          cypher_agent=cypher_agent,
          synthesis_agent=synthesis_agent,
      )

      # -- Vector RAG retriever ----------------------------------------
      logger.info("Initialising VectorRetriever (ChromaDB) …")
      vector_retriever = VectorRetriever()

      # ==============================================================
      # 2. Execution
      # ==============================================================
      logger.info("Loading ground-truth test cases …")
      cases = get_thesis_test_cases(target_hop=args.hop)
      logger.info("Loaded %d test cases.", len(cases))

      logger.info("Instantiating EvaluationRunner …")
      runner = EvaluationRunner(
          graph_rag=orchestrator,
          vector_retriever=vector_retriever,
          synthesis_agent=synthesis_agent,
          export_path=raw_csv_path,
      )

      logger.info("Running evaluation across both pipelines …")
      results = runner.run_all(cases)

      logger.info("Exporting raw results to DataFrame …")
      raw_df: pd.DataFrame = runner.export_to_pandas(results)
      logger.info("Raw results shape: %s", raw_df.shape)

      # ==============================================================
      # 3. Metric Calculation (RAGAS)
      # ==============================================================
      logger.info("Instantiating RagasEvaluator …")
      evaluator = RagasEvaluator(
          evaluator_llm=evaluator_llm,
          evaluator_embeddings=evaluator_embeddings,
      )
      
      logger.info("Scoring results with RAGAS metrics …")
      scored_df: pd.DataFrame = evaluator.evaluate_dataframe(raw_df)

      # Persist scored results (clean write per hop)
      scored_csv_path.parent.mkdir(parents=True, exist_ok=True)
      scored_df.to_csv(
          scored_csv_path, mode="w", header=True, index=False, encoding="utf-8"
      )
      logger.info("Exported %d scored results to %s", len(scored_df), scored_csv_path)

      # ==============================================================
      # 4. Summary
      # ==============================================================
      logger.info("Generating evaluation summary for hop %d …", args.hop)

      summary_df: pd.DataFrame = evaluator.generate_thesis_summary(scored_df)

      # -- Console output ------------------------------------------------
      print(
          f"\n=== TELECOM OPERATIONAL INTELLIGENCE: HOP {args.hop} RESULTS ===\n"
      )
      with pd.option_context(
          "display.max_columns", None,
          "display.width", 160,
          "display.float_format", "{:.4f}".format,
      ):
          print(scored_df.to_string(index=False))

      print(
          f"\n=== SUMMARY: Mean Metrics by Pipeline (Hop {args.hop}) ===\n"
      )
      with pd.option_context(
          "display.max_columns", None,
          "display.width", 160,
          "display.float_format", "{:.4f}".format,
      ):
          print(summary_df.to_string(index=False))

      print(f"\nScored CSV saved to: {scored_csv_path}")
      print(f"Debug logs saved to: {_LOG_FILE_PATH}")
      logger.info("Evaluation pipeline completed successfully.")

    finally:
        # Guarantee the Neo4j driver is closed even on failure
        neo4j_conn.__exit__(None, None, None)
        logger.info("Neo4j connection closed.")


if __name__ == "__main__":
    main()
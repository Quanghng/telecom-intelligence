"""
extractor.py – CSVExtractor
=============================
Memory-efficient, generator-based CSV reader for the Telecom ETL pipeline.

Uses Python's built-in :mod:`csv` module and the **yield** pattern to
stream arbitrarily large CSV files in fixed-size batches, avoiding the
need to load the entire dataset into memory at once.

Usage
-----
    >>> from src.ingestion import CSVExtractor
    >>> extractor = CSVExtractor()
    >>> for batch in extractor.extract_in_batches("data/raw/nodes.csv", batch_size=200):
    ...     print(f"Batch of {len(batch)} rows")
"""

from __future__ import annotations

import csv
import logging
from typing import Any, Dict, Iterator, List

logger: logging.Logger = logging.getLogger(__name__)


class CSVExtractor:
    """Batch-yielding CSV reader.

    Reads a CSV file via :class:`csv.DictReader` and yields rows in
    fixed-size batches, keeping peak memory usage proportional to
    ``batch_size`` rather than to total file size.

    Examples
    --------
    >>> extractor = CSVExtractor()
    >>> for batch in extractor.extract_in_batches("data/raw/edges.csv"):
    ...     process(batch)
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_in_batches(
        self,
        file_path: str,
        batch_size: int = 2000,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield rows from *file_path* in batches of *batch_size*.

        Parameters
        ----------
        file_path : str
            Path to the CSV file (relative or absolute).
        batch_size : int, optional
            Maximum number of rows per yielded batch (default ``2000``).

        Yields
        ------
        List[Dict[str, Any]]
            A list of up to *batch_size* dictionaries, one per CSV row,
            keyed by the header column names.

        Raises
        ------
        FileNotFoundError
            If *file_path* does not exist.
        IOError
            If the file cannot be read for any other I/O reason.
        """
        logger.info("Opening CSV file: %s (batch_size=%d)", file_path, batch_size)

        try:
            with open(file_path, mode="r", encoding="utf-8", newline="") as fh:
                reader: csv.DictReader = csv.DictReader(fh)
                batch: List[Dict[str, Any]] = []
                row_count: int = 0
                batch_number: int = 0

                for row in reader:
                    batch.append(dict(row))
                    row_count += 1

                    if len(batch) >= batch_size:
                        batch_number += 1
                        logger.debug(
                            "Yielding batch #%d  (%d rows, cumulative %d)",
                            batch_number,
                            len(batch),
                            row_count,
                        )
                        yield batch
                        batch = []

                # Yield any remaining rows that didn't fill a full batch.
                if batch:
                    batch_number += 1
                    logger.debug(
                        "Yielding final batch #%d  (%d rows, cumulative %d)",
                        batch_number,
                        len(batch),
                        row_count,
                    )
                    yield batch

                logger.info(
                    "CSV extraction complete: %s → %d row(s) in %d batch(es).",
                    file_path,
                    row_count,
                    batch_number,
                )

        except FileNotFoundError:
            logger.error("CSV file not found: %s", file_path)
            raise
        except IOError as exc:
            logger.error("I/O error reading CSV file %s: %s", file_path, exc)
            raise

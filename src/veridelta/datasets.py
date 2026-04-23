# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Built-in datasets for Veridelta testing and quickstart examples.

This module provides utilities to securely download, cache, and load sample
datasets used in Veridelta's documentation and tutorials.
"""

import logging
import pathlib
import urllib.error
import urllib.request

import polars as pl

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# TODO: Update this URL to the GitHub Release URL once the dataset is published there.
_TAXI_URL = "https://raw.githubusercontent.com/Veridelta/veridelta/main/docs/assets/data/sample_taxi_data.parquet"


def _get_cache_dir() -> pathlib.Path:
    """Get the local cache directory for Veridelta datasets.

    Returns:
        pathlib.Path: The path to the local cache directory.
    """
    cache_dir = pathlib.Path.home() / ".cache" / "veridelta" / "datasets"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def load_nyc_taxi() -> pl.DataFrame:
    """Load the NYC Taxi sample dataset.

    Downloads the dataset from the official Veridelta repository and caches it
    locally to ensure high-speed subsequent loads.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the NYC Taxi sample data.

    Raises:
        RuntimeError: If the download fails due to network or routing issues.
    """
    cache_path = _get_cache_dir() / "sample_taxi_data.parquet"

    if not cache_path.exists():
        logger.warning(f"Downloading NYC Taxi dataset to {cache_path}...")
        try:
            urllib.request.urlretrieve(_TAXI_URL, cache_path)
        except urllib.error.URLError as e:
            raise RuntimeError(
                f"Failed to download Veridelta sample dataset. "
                f"Check your internet connection or the URL. Error: {e}"
            ) from e

    return pl.read_parquet(cache_path)

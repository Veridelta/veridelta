# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Built-in datasets for Veridelta testing and quickstart examples.

This module provides utilities to securely download, cache, and load sample
datasets used in Veridelta's documentation and tutorials.
"""

import importlib.metadata
import logging
import pathlib
import shutil
import urllib.error
import urllib.request

import polars as pl

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

try:
    __version__ = importlib.metadata.version("veridelta")
    git_ref = f"v{__version__}"
except importlib.metadata.PackageNotFoundError:
    git_ref = "main"

_GIT_REF = git_ref

_TAXI_URL = f"https://raw.githubusercontent.com/Veridelta/veridelta/{_GIT_REF}/docs/assets/data/sample_taxi_data.parquet"


def _get_cache_dir() -> pathlib.Path:
    """Resolve and create the local cache directory for Veridelta datasets.

    Returns:
        pathlib.Path: The path to the local cache directory.
    """
    cache_dir = pathlib.Path.home() / ".cache" / "veridelta" / "datasets"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def load_nyc_taxi() -> pl.LazyFrame:
    """Load the NYC Taxi sample dataset into a lazy computation graph.

    Download the dataset from the official Veridelta repository and cache it
    locally for subsequent runs. Evaluates lazily to ensure zero memory footprint
    until execution.

    Returns:
        pl.LazyFrame: A Polars LazyFrame pointing to the cached dataset.

    Raises:
        RuntimeError: If the download fails due to network or routing issues.

    Notes:
        A 15-second timeout is enforced during the download. To prevent cache
        corruption, the file is downloaded to a temporary location and atomically
        renamed only upon successful completion.

    Examples:
        Load the sample dataset to rapidly prototype a comparison:

        ```python
        from veridelta.datasets import load_nyc_taxi

        lf = load_nyc_taxi()

        # Execute the graph to view the first 5 rows
        print(lf.head(5).collect())
        ```
    """
    cache_path = _get_cache_dir() / "sample_taxi_data.parquet"

    def _download_file() -> None:
        logger.warning(f"Downloading NYC Taxi dataset to {cache_path}...")
        tmp_path = cache_path.with_suffix(".parquet.tmp")

        try:
            req = urllib.request.Request(_TAXI_URL)
            with (
                urllib.request.urlopen(req, timeout=15.0) as response,
                open(tmp_path, "wb") as out_file,
            ):
                shutil.copyfileobj(response, out_file)

            tmp_path.rename(cache_path)

        except urllib.error.URLError as e:
            if tmp_path.exists():
                tmp_path.unlink()
            raise RuntimeError(
                f"Failed to download Veridelta sample dataset. "
                f"Check your internet connection or the URL. Error: {e}"
            ) from e

    if not cache_path.exists():
        _download_file()

    try:
        lf = pl.scan_parquet(cache_path)
        lf.collect_schema()
        return lf
    except Exception:
        logger.warning("Cached dataset is corrupted. Evicting and re-downloading...")
        if cache_path.exists():
            cache_path.unlink()

        _download_file()
        return pl.scan_parquet(cache_path)

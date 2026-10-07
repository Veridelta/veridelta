# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Sample datasets for the tutorials and documentation examples."""

import hashlib
import importlib.metadata
import logging
import pathlib
import urllib.error
import urllib.request
from typing import Final

import polars as pl

from veridelta.exceptions import DatasetError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _git_ref() -> str:
    """Name the ref the sample is read from: the installed release, or `main` from a checkout."""
    try:
        return f"v{importlib.metadata.version('veridelta')}"
    except importlib.metadata.PackageNotFoundError:
        return "main"


_GIT_REF = _git_ref()

_TAXI_URL = f"https://raw.githubusercontent.com/Veridelta/veridelta/{_GIT_REF}/docs/assets/data/sample_taxi_data.parquet"

_TAXI_SHA256: Final = "f4eaf27ea5b6d0d91fe1bbe34c22180c3ebaf68d465e72eafc4d9e71d3567178"
"""The SHA-256 of `docs/assets/data/sample_taxi_data.parquet`, which a test holds to the file."""

_TAXI_MAX_BYTES: Final = 1 << 20
"""The most bytes a download may hold. The sample is about 32 KB."""

_CHUNK_BYTES: Final = 1 << 16


def _get_cache_dir() -> pathlib.Path:
    """Return the local cache directory for Veridelta datasets."""
    cache_dir = pathlib.Path.home() / ".cache" / "veridelta" / "datasets"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _is_the_sample(path: pathlib.Path) -> bool:
    """Return whether a file holds the published sample, byte for byte."""
    return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == _TAXI_SHA256


def _download(cache_path: pathlib.Path) -> None:
    """Download the sample next to `cache_path`, check it, and only then move it into place.

    Raises:
        DatasetError: If the download fails, passes the size cap, or is not
            the published sample.
    """
    logger.info("Downloading the NYC taxi sample to %s", cache_path)
    partial = cache_path.with_name(f"{cache_path.name}.part")
    digest = hashlib.sha256()
    size = 0
    try:
        with (
            urllib.request.urlopen(urllib.request.Request(_TAXI_URL), timeout=15.0) as response,
            open(partial, "wb") as out_file,
        ):
            while chunk := response.read(_CHUNK_BYTES):
                size += len(chunk)
                if size > _TAXI_MAX_BYTES:
                    raise DatasetError(
                        f"The sample download passed {_TAXI_MAX_BYTES:,} bytes, which the "
                        "published sample never reaches, so it was stopped and not cached."
                    )
                digest.update(chunk)
                out_file.write(chunk)
    except urllib.error.URLError as e:
        partial.unlink(missing_ok=True)
        cache_path.unlink(missing_ok=True)
        raise DatasetError(
            f"Failed to download Veridelta sample dataset. "
            f"Check your internet connection or the URL. Error: {e}"
        ) from e
    except DatasetError:
        partial.unlink(missing_ok=True)
        raise
    if digest.hexdigest() != _TAXI_SHA256:
        partial.unlink(missing_ok=True)
        raise DatasetError(
            f"The file downloaded from {_TAXI_URL} is not the published sample: its SHA-256 "
            "differs, so it was not cached."
        )
    partial.replace(cache_path)


def load_nyc_taxi() -> pl.DataFrame:
    """Load the NYC Taxi sample dataset.

    The file is downloaded from the Veridelta repository once and cached under
    `~/.cache/veridelta/datasets`. A download must match the sample's pinned
    SHA-256, and stay under 1 MiB, before it is cached, and a cached copy that
    no longer matches, such as a corrupt one, is downloaded again. A download
    times out after 15 seconds.

    Returns:
        pl.DataFrame: The sample trips.

    Raises:
        DatasetError: If the download fails, or is not the published sample.
    """
    cache_path = _get_cache_dir() / "sample_taxi_data.parquet"
    if not _is_the_sample(cache_path):
        if cache_path.exists():
            logger.warning(
                "The cached NYC taxi sample differs from the published one; it is evicted "
                "and downloaded again."
            )
            cache_path.unlink()
        _download(cache_path)
    return pl.read_parquet(cache_path)

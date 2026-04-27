# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Veridelta dataset utilities and cache management."""

import io
import urllib.error
from email.message import Message
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.datasets import _get_cache_dir, load_nyc_taxi  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
@pytest.mark.fast
class TestDatasetCacheManagement:
    """Validate secure downloading, file system caching, and network fallbacks."""

    def test_it_creates_and_returns_the_local_cache_directory(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure the cache directory is properly resolved and created inside the user's home folder."""
        mocker.patch("veridelta.datasets.pathlib.Path.home", return_value=tmp_path)

        cache_dir = _get_cache_dir()

        expected_dir = tmp_path / ".cache" / "veridelta" / "datasets"
        assert cache_dir == expected_dir
        assert cache_dir.exists()
        assert cache_dir.is_dir()

    def test_it_loads_dataset_directly_from_cache_without_downloading_when_file_exists(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure subsequent loads bypass the network and read directly from disk."""
        mocker.patch("veridelta.datasets._get_cache_dir", return_value=tmp_path)
        mock_urlopen = mocker.patch("veridelta.datasets.urllib.request.urlopen")

        df = pl.DataFrame({"trip_id": [1, 2, 3]})
        cache_file = tmp_path / "sample_taxi_data.parquet"
        df.write_parquet(cache_file)

        result_df = load_nyc_taxi()

        mock_urlopen.assert_not_called()
        assert result_df.height == 3
        assert result_df.columns == ["trip_id"]

    def test_it_downloads_and_saves_dataset_when_cache_is_empty(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure missing datasets trigger a targeted download and save the file permanently."""
        mocker.patch("veridelta.datasets._get_cache_dir", return_value=tmp_path)

        # Generate an in-memory parquet byte stream to act as a "download"
        df = pl.DataFrame({"downloaded_col": ["A", "B"]})
        parquet_bytes = io.BytesIO()
        df.write_parquet(parquet_bytes)
        parquet_bytes.seek(0)

        mock_response = MagicMock()
        mock_response.__enter__.return_value = parquet_bytes
        mock_urlopen = mocker.patch(
            "veridelta.datasets.urllib.request.urlopen", return_value=mock_response
        )

        result_df = load_nyc_taxi()

        mock_urlopen.assert_called_once()

        args, kwargs = mock_urlopen.call_args
        request_obj = args[0]
        assert request_obj.full_url.startswith("https://raw.githubusercontent.com/Veridelta")
        assert kwargs["timeout"] == 15.0

        assert (tmp_path / "sample_taxi_data.parquet").exists()

        assert result_df.columns == ["downloaded_col"]
        assert result_df.height == 2

    def test_it_cleans_up_partial_files_and_raises_runtime_error_on_download_failure(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure corrupted or interrupted downloads do not leave broken artifact files on disk."""
        mocker.patch("veridelta.datasets._get_cache_dir", return_value=tmp_path)
        cache_file = tmp_path / "sample_taxi_data.parquet"

        # Simulate a file partially writing before the network dies
        def simulate_network_failure(*args, **kwargs):  # type: ignore[no-untyped-def]
            cache_file.touch()  # Simulate 0-byte / partial write
            raise urllib.error.URLError("Network unreachable")

        mock_urlopen = mocker.patch("veridelta.datasets.urllib.request.urlopen")
        mock_urlopen.side_effect = simulate_network_failure

        with pytest.raises(RuntimeError, match="Failed to download Veridelta sample dataset"):
            load_nyc_taxi()

        assert not cache_file.exists()

    def test_it_handles_http_errors_like_404_not_found_gracefully(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure HTTP rejections (like a bad GitHub ref) trigger cleanup and error handling."""
        mocker.patch("veridelta.datasets._get_cache_dir", return_value=tmp_path)
        cache_file = tmp_path / "sample_taxi_data.parquet"

        http_error = urllib.error.HTTPError(
            url="http://fake",
            code=404,
            msg="Not Found",
            hdrs=Message(),
            fp=None,
        )

        # Simulate the file being opened for writing right before the HTTPError triggers
        def simulate_404(*args, **kwargs):  # type: ignore[no-untyped-def]
            cache_file.touch()
            raise http_error

        mock_urlopen = mocker.patch("veridelta.datasets.urllib.request.urlopen")
        mock_urlopen.side_effect = simulate_404

        with pytest.raises(RuntimeError, match="Check your internet connection or the URL"):
            load_nyc_taxi()

        # Assert the 0-byte file was deleted so the next run can try again
        assert not cache_file.exists()

    def test_it_automatically_evicts_corrupted_cache_and_redownloads_the_file(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure corrupted cache files are automatically deleted and re-downloaded."""
        mocker.patch("veridelta.datasets._get_cache_dir", return_value=tmp_path)

        cache_file = tmp_path / "sample_taxi_data.parquet"
        cache_file.write_text("This is definitely not a valid parquet binary.")

        df = pl.DataFrame({"recovered_col": [1, 2]})
        parquet_bytes = io.BytesIO()
        df.write_parquet(parquet_bytes)
        parquet_bytes.seek(0)

        mock_response = MagicMock()
        mock_response.__enter__.return_value = parquet_bytes
        mock_urlopen = mocker.patch(
            "veridelta.datasets.urllib.request.urlopen", return_value=mock_response
        )

        result_df = load_nyc_taxi()

        # The network SHOULD be called oncebecause it realized the cache was corrupt
        mock_urlopen.assert_called_once()
        assert result_df.columns == ["recovered_col"]
        assert result_df.height == 2

"""Core engine for data ingestion and comparison."""

from abc import ABC, abstractmethod

import polars as pl

from veridelta.models import SourceConfig, SourceType


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Load data into a Polars DataFrame based on config."""
        pass


class CSVLoader(BaseLoader):
    """Loader for CSV files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Reads CSV using Polars, passing through any custom options."""
        return pl.read_csv(config.path, **config.options)


class ParquetLoader(BaseLoader):
    """Loader for Parquet files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Reads Parquet using Polars."""
        return pl.read_parquet(config.path, **config.options)


class LoaderFactory:
    """Factory to return the appropriate loader based on SourceType."""

    _loaders: dict[SourceType, BaseLoader] = {
        SourceType.CSV: CSVLoader(),
        SourceType.PARQUET: ParquetLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: SourceType) -> BaseLoader:
        """Returns the loader for the given type.

        Raises:
            NotImplementedError: If the format is on the roadmap but not coded yet.
        """
        loader = cls._loaders.get(source_type)
        if not loader:
            raise NotImplementedError(
                f"Support for '{source_type}' is planned but not yet implemented."
            )
        return loader

"""Core engine for data ingestion and alignment."""

from abc import ABC, abstractmethod

import polars as pl

from veridelta.models import DiffConfig, SourceConfig, SourceType


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Load data into a Polars DataFrame.

        Args:
            config: The source configuration containing path and options.

        Returns:
            A Polars DataFrame containing the loaded data.
        """
        pass


class CSVLoader(BaseLoader):
    """Loader for CSV files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Reads CSV using Polars read_csv.

        Args:
            config: Configuration including file path and separator options.

        Returns:
            The loaded Polars DataFrame.
        """
        return pl.read_csv(config.path, **config.options)


class ParquetLoader(BaseLoader):
    """Loader for Parquet files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Reads Parquet using Polars read_parquet.

        Args:
            config: Configuration including file path.

        Returns:
            The loaded Polars DataFrame.
        """
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

        Args:
            source_type: The enum value representing the file format.

        Returns:
            A concrete implementation of BaseLoader.

        Raises:
            NotImplementedError: If the format is not yet supported.
        """
        loader = cls._loaders.get(source_type)
        if not loader:
            raise NotImplementedError(
                f"Support for '{source_type}' is planned but not yet implemented."
            )
        return loader


class DataIngestor:
    """Coordinates loading and alignment of source and target datasets."""

    def __init__(self, config: DiffConfig) -> None:
        """Initialize the DataIngestor.

        Args:
            config: The master configuration defining sources and rules.
        """
        self.config = config

    def _align_columns(self, df: pl.DataFrame, is_source: bool = True) -> pl.DataFrame:
        """Applies renames and drops ignored columns.

        Args:
            df: The raw input DataFrame.
            is_source: Whether this is the 'Source' (Left) dataset.

        Returns:
            A DataFrame standardized to the target schema.
        """
        rename_map = {}
        to_drop = []

        for rule in self.config.rules:
            if rule.ignore:
                if rule.name in df.columns:
                    to_drop.append(rule.name)
                continue

            if is_source and rule.rename_to and rule.name in df.columns:
                rename_map[rule.name] = rule.rename_to

        return df.drop(to_drop).rename(rename_map)

    def get_dataframes(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Loads and aligns both datasets.

        Returns:
            A tuple containing (source_df, target_df).
        """
        source_loader = LoaderFactory.get_loader(self.config.source.format)
        target_loader = LoaderFactory.get_loader(self.config.target.format)

        source_df = source_loader.load(self.config.source)
        target_df = target_loader.load(self.config.target)

        source_df = self._align_columns(source_df, is_source=True)
        target_df = self._align_columns(target_df, is_source=False)

        return source_df, target_df

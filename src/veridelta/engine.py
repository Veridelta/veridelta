"""Core engine for data ingestion and alignment."""

from abc import ABC, abstractmethod

import polars as pl

from veridelta.models import DiffConfig, DiffSummary, SourceConfig, SourceType


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


class DiffEngine:
    """The core mathematical engine that calculates differences."""

    def __init__(
        self, config: DiffConfig, source_df: pl.DataFrame, target_df: pl.DataFrame
    ) -> None:
        """Initialize the DiffEngine.

        Args:
            config: The comparison configuration.
            source_df: The aligned source DataFrame.
            target_df: The aligned target DataFrame.
        """
        self.config = config
        self.source = source_df
        self.target = target_df

    def run(self) -> DiffSummary:
        """Executes the comparison logic.

        Returns:
            A DiffSummary object containing the counts of changes.
        """
        combined = self.source.join(
            self.target, on=self.config.primary_keys, how="full", suffix="_target"
        )

        pk = self.config.primary_keys[0]

        # Rows where the primary key is missing from source = Added
        added = combined.filter(pl.col(pk).is_null())
        # Rows where the primary key is missing from target = Removed
        removed = combined.filter(pl.col(f"{pk}_target").is_null())
        common = combined.filter(pl.col(pk).is_not_null() & pl.col(f"{pk}_target").is_not_null())

        # For now, count a row as 'changed' if ANY non-PK column differs
        changed_count = 0
        if not common.is_empty():
            # Simple check for now
            for column in self.source.columns:
                if column in self.config.primary_keys:
                    continue

                diff_mask = common.filter(pl.col(column) != pl.col(f"{column}_target"))
                if not diff_mask.is_empty():
                    changed_count = diff_mask.height
                    break  # Optimization: one change makes the whole row 'changed'

        return DiffSummary(
            total_rows_source=self.source.height,
            total_rows_target=self.target.height,
            added_count=added.height,
            removed_count=removed.height,
            changed_count=changed_count,
            is_match=(added.height == 0 and removed.height == 0 and changed_count == 0),
        )

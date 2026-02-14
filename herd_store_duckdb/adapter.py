"""DuckDB implementation of StoreAdapter protocol."""

from __future__ import annotations


class DuckDBStoreAdapter:
    """Storage adapter backed by DuckDB.

    Implements the StoreAdapter protocol from herd-core.
    """

    def __init__(self, path: str = ":memory:", schema: str = "herd") -> None:
        """Initialize the DuckDB storage adapter.

        Args:
            path: Database file path, or ":memory:" for in-memory database.
            schema: Schema name for Herd tables (default: "herd").
        """
        self.path = path
        self.schema = schema

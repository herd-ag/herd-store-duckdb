"""Tests for DuckDBStoreAdapter.storage_info() method."""

import os
import tempfile
from datetime import datetime


from herd_store_duckdb import DuckDBStoreAdapter


def test_storage_info_memory_database() -> None:
    """Test storage_info() for in-memory database."""
    store = DuckDBStoreAdapter(":memory:")
    info = store.storage_info()

    assert info["path"] == ":memory:"
    assert info["size_bytes"] == 0
    assert info["last_modified"] == ""


def test_storage_info_file_database_exists() -> None:
    """Test storage_info() for existing file-based database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        store = DuckDBStoreAdapter(db_path)

        # Create some data to ensure file exists
        from herd_core.types import AgentRecord, AgentState

        agent = AgentRecord(
            id="test-123",
            agent="mason",
            model="claude-sonnet-4-5",
            state=AgentState.RUNNING,
        )
        store.save(agent)

        # Get storage info
        info = store.storage_info()

        assert info["path"] == db_path
        assert isinstance(info["size_bytes"], int)
        assert info["size_bytes"] > 0  # File should have some content
        assert isinstance(info["last_modified"], str)
        assert info["last_modified"] != ""

        # Verify last_modified is a valid ISO 8601 timestamp
        datetime.fromisoformat(info["last_modified"])


def test_storage_info_file_database_created_immediately() -> None:
    """Test storage_info() for file-based database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")

        # DuckDB creates the file immediately when connecting
        store = DuckDBStoreAdapter(db_path)

        info = store.storage_info()

        assert info["path"] == db_path
        # DuckDB allocates space immediately, so size > 0
        assert info["size_bytes"] > 0
        assert isinstance(info["last_modified"], str)
        assert info["last_modified"] != ""

        # Verify it's a valid timestamp
        datetime.fromisoformat(info["last_modified"])


def test_storage_info_matches_os_stat() -> None:
    """Test that storage_info() matches os.stat() for file-based database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        store = DuckDBStoreAdapter(db_path)

        # Create some data
        from herd_core.types import AgentRecord, AgentState

        agent = AgentRecord(
            id="test-456",
            agent="mason",
            state=AgentState.COMPLETED,
        )
        store.save(agent)

        # Get storage info
        info = store.storage_info()

        # Compare with os.stat()
        stat = os.stat(db_path)

        assert info["size_bytes"] == stat.st_size

        # Parse the ISO timestamp and compare (within 1 second tolerance)
        info_mtime = datetime.fromisoformat(info["last_modified"])
        stat_mtime = datetime.fromtimestamp(stat.st_mtime, tz=info_mtime.tzinfo)

        # Allow 1 second difference due to precision differences
        time_diff = abs((info_mtime - stat_mtime).total_seconds())
        assert time_diff < 1.0


def test_storage_info_path_accuracy() -> None:
    """Test that storage_info() returns accurate path information."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        store = DuckDBStoreAdapter(db_path)

        # Create initial data
        from herd_core.types import AgentRecord, AgentState

        agent1 = AgentRecord(
            id="test-1",
            agent="mason",
            state=AgentState.RUNNING,
        )
        store.save(agent1)

        info1 = store.storage_info()

        # Verify path matches
        assert info1["path"] == db_path

        # Verify file exists at that path
        assert os.path.exists(db_path)

        # Add more data
        agent2 = AgentRecord(
            id="test-2",
            agent="fresco",
            state=AgentState.COMPLETED,
        )
        store.save(agent2)

        info2 = store.storage_info()

        # Path should remain the same
        assert info2["path"] == db_path

        # Size should be reported (DuckDB may allocate in fixed blocks)
        assert info2["size_bytes"] >= info1["size_bytes"]


def test_storage_info_return_types() -> None:
    """Test that storage_info() returns correct types."""
    store = DuckDBStoreAdapter(":memory:")
    info = store.storage_info()

    # Check return type is dict
    assert isinstance(info, dict)

    # Check required keys exist
    assert "path" in info
    assert "size_bytes" in info
    assert "last_modified" in info

    # Check value types
    assert isinstance(info["path"], str)
    assert isinstance(info["size_bytes"], int)
    assert isinstance(info["last_modified"], str)

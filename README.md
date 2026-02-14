# herd-store-duckdb

DuckDB storage adapter for The Herd.

Implements the `StoreAdapter` protocol from `herd-core` for persisting and retrieving Herd domain records (agents, tickets, PRs, decisions, reviews, models, sprints) and activity events (lifecycle, ticket state changes, PR activity, reviews, token usage).

## Installation

```bash
pip install herd-store-duckdb
```

## Usage

```python
from herd_store_duckdb import DuckDBStoreAdapter
from herd_core.types import AgentRecord, AgentState

# In-memory database
store = DuckDBStoreAdapter(":memory:")

# File-based database
store = DuckDBStoreAdapter("herd.duckdb")

# Save an entity
agent = AgentRecord(
    id="abc-123",
    agent="grunt",
    model="claude-sonnet-4-5",
    ticket_id="DBC-145",
    state=AgentState.RUNNING
)
store.save(agent)

# Retrieve it
agent = store.get(AgentRecord, "abc-123")

# List with filters
agents = store.list(AgentRecord, state=AgentState.RUNNING, active=True)

# Count
count = store.count(AgentRecord, active=True)
```

## Schema

The adapter manages a `herd` schema with tables for all Entity and Event types defined in `herd-core`. Tables are created automatically on first use.

## License

MIT - Copyright (c) 2026 Faust Eriksen

"""DuckDB implementation of StoreAdapter protocol."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any

import duckdb
from herd_core.adapters.store import StoreAdapter
from herd_core.types import (
    AgentRecord,
    DecisionRecord,
    Entity,
    Event,
    LifecycleEvent,
    ModelRecord,
    PREvent,
    PRRecord,
    ReviewEvent,
    ReviewRecord,
    SprintRecord,
    TicketEvent,
    TicketRecord,
    TokenEvent,
)


# Type dispatch registries
ENTITY_TABLE_MAP: dict[type[Entity], str] = {
    AgentRecord: "agent_instance",
    TicketRecord: "ticket",
    PRRecord: "pull_request",
    DecisionRecord: "decision_record",
    ReviewRecord: "review_finding",
    ModelRecord: "model_def",
    SprintRecord: "sprint",
}

EVENT_TABLE_MAP: dict[type[Event], str] = {
    LifecycleEvent: "lifecycle_event",
    TicketEvent: "ticket_event",
    PREvent: "pr_event",
    ReviewEvent: "review_event",
    TokenEvent: "token_event",
}


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
        self.conn = duckdb.connect(path)

        # Create schema
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Initialize tables on first use
        self._initialized_tables: set[str] = set()

    def _ensure_table(self, table_name: str, entity_type: type[Entity] | type[Event]) -> None:
        """Create table if it doesn't exist.

        Args:
            table_name: The table name (without schema).
            entity_type: The Entity or Event type to create the table for.
        """
        if table_name in self._initialized_tables:
            return

        # Get all fields from the dataclass
        fields = dataclasses.fields(entity_type)

        # Build column definitions
        columns = []
        for field in fields:
            col_type = self._get_sql_type(field.type)
            # Add PRIMARY KEY constraint to id field for entities
            if field.name == "id" and issubclass(entity_type, Entity):
                columns.append(f"{field.name} {col_type} PRIMARY KEY")
            else:
                columns.append(f"{field.name} {col_type}")

        columns_sql = ",\n    ".join(columns)
        qualified_table = f"{self.schema}.{table_name}"

        sql = f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            {columns_sql}
        )
        """
        self.conn.execute(sql)
        self._initialized_tables.add(table_name)

    def _get_sql_type(self, python_type: Any) -> str:
        """Map Python types to DuckDB SQL types.

        Args:
            python_type: The Python type annotation.

        Returns:
            SQL type string.
        """
        # Handle string representation of types
        type_str = str(python_type)

        # datetime types
        if "datetime" in type_str:
            return "TIMESTAMP"

        # Decimal types
        if "Decimal" in type_str:
            return "DECIMAL(18,10)"

        # Enum types
        if "AgentState" in type_str:
            return "VARCHAR"

        # TicketPriority is an IntEnum - store as INTEGER
        if "TicketPriority" in type_str:
            return "INTEGER"

        # List types
        if "list" in type_str:
            return "VARCHAR[]"

        # Integer types
        if type_str in ("int", "<class 'int'>") or "int" in type_str:
            return "INTEGER"

        # Float types
        if type_str in ("float", "<class 'float'>") or "float" in type_str:
            return "DOUBLE"

        # Boolean types
        if type_str in ("bool", "<class 'bool'>") or "bool" in type_str:
            return "BOOLEAN"

        # Default to VARCHAR for strings and other types
        return "VARCHAR"

    def _get_table_name(self, entity_type: type[Entity] | type[Event]) -> str:
        """Get the table name for an entity or event type.

        Args:
            entity_type: The Entity or Event type.

        Returns:
            Table name (without schema).
        """
        if entity_type in ENTITY_TABLE_MAP:
            return ENTITY_TABLE_MAP[entity_type]
        if entity_type in EVENT_TABLE_MAP:
            return EVENT_TABLE_MAP[entity_type]
        raise ValueError(f"Unknown entity type: {entity_type}")

    def _build_where_clause(self, filters: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build WHERE clause from filters.

        Args:
            filters: Filter dictionary.

        Returns:
            Tuple of (WHERE clause string, parameters list).
        """
        if not filters:
            return "", []

        conditions = []
        params = []

        for key, value in filters.items():
            if key == "active":
                # Special case: active=True means deleted_at IS NULL
                if value:
                    conditions.append("deleted_at IS NULL")
            elif key == "since":
                # Special case for event queries: created_at >= since
                conditions.append("created_at >= ?")
                params.append(value)
            else:
                # Handle enum values
                if hasattr(value, "value"):
                    value = value.value
                conditions.append(f"{key} = ?")
                params.append(value)

        where_clause = " AND ".join(conditions)
        return f"WHERE {where_clause}" if where_clause else "", params

    def _row_to_entity(self, entity_type: type[Entity] | type[Event], row: tuple) -> Entity | Event:
        """Convert a database row to an entity instance.

        Args:
            entity_type: The Entity or Event type.
            row: Database row tuple.

        Returns:
            Entity or Event instance.
        """
        fields = dataclasses.fields(entity_type)
        field_names = [f.name for f in fields]

        # Build kwargs dict from row
        kwargs = {}
        for i, field_name in enumerate(field_names):
            value = row[i]
            field_type = fields[i].type

            # Handle enum conversion
            type_str = str(field_type)
            if "AgentState" in type_str and value is not None:
                from herd_core.types import AgentState

                value = AgentState(value)
            elif "TicketPriority" in type_str and value is not None:
                from herd_core.types import TicketPriority

                value = TicketPriority(value)

            kwargs[field_name] = value

        return entity_type(**kwargs)

    def get(self, entity_type: type[Entity], id: str) -> Entity | None:
        """Retrieve a single entity by ID.

        Args:
            entity_type: The Entity subclass to retrieve (e.g., AgentRecord).
            id: The entity's unique identifier.

        Returns:
            The entity instance, or None if not found (or soft-deleted).
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        sql = f"SELECT * FROM {qualified_table} WHERE id = ? AND deleted_at IS NULL"

        result = self.conn.execute(sql, [id]).fetchone()
        if result is None:
            return None

        return self._row_to_entity(entity_type, result)

    def list(self, entity_type: type[Entity], **filters: Any) -> list[Entity]:
        """List entities matching filters.

        Args:
            entity_type: The Entity subclass to list (e.g., TicketRecord).
            **filters: Field-value pairs to filter on.

        Returns:
            List of matching entities. Empty list if none match.
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(filters)

        sql = f"SELECT * FROM {qualified_table} {where_clause}"
        results = self.conn.execute(sql, params).fetchall()

        return [self._row_to_entity(entity_type, row) for row in results]

    def save(self, record: Entity) -> str:
        """Save an entity (insert or update).

        If the entity's id exists in the store, updates it.
        If not, inserts it. Sets modified_at automatically.

        Args:
            record: The entity to persist.

        Returns:
            The entity's id.
        """
        entity_type = type(record)
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        # Set modified_at
        if hasattr(record, "modified_at"):
            record.modified_at = datetime.now(timezone.utc)

        # Get all fields
        fields = dataclasses.fields(entity_type)
        field_names = [f.name for f in fields]

        # Build values list
        values = []
        for field in fields:
            value = getattr(record, field.name)
            # Convert enum to value
            if hasattr(value, "value"):
                value = value.value
            values.append(value)

        # Build INSERT OR REPLACE
        qualified_table = f"{self.schema}.{table_name}"
        placeholders = ", ".join(["?" for _ in field_names])
        columns = ", ".join(field_names)

        sql = f"INSERT OR REPLACE INTO {qualified_table} ({columns}) VALUES ({placeholders})"
        self.conn.execute(sql, values)

        return record.id

    def delete(self, entity_type: type[Entity], id: str) -> None:
        """Soft-delete an entity by setting deleted_at.

        Does not physically remove the record.

        Args:
            entity_type: The Entity type.
            id: The entity's unique identifier.
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        deleted_at = datetime.now(timezone.utc)

        sql = f"UPDATE {qualified_table} SET deleted_at = ? WHERE id = ?"
        self.conn.execute(sql, [deleted_at, id])

    def append(self, event: Event) -> None:
        """Append an immutable event to the activity ledger.

        Events are never updated or deleted. This is the audit trail.
        Sets created_at automatically if not provided.

        Args:
            event: The event to append.
        """
        event_type = type(event)
        table_name = self._get_table_name(event_type)
        self._ensure_table(table_name, event_type)

        # Set created_at if not provided
        if event.created_at is None:
            # Events are frozen dataclasses, so we need to use object.__setattr__
            object.__setattr__(event, "created_at", datetime.now(timezone.utc))

        # Get all fields
        fields = dataclasses.fields(event_type)
        field_names = [f.name for f in fields]

        # Build values list
        values = []
        for field in fields:
            value = getattr(event, field.name)
            # Convert enum to value
            if hasattr(value, "value"):
                value = value.value
            values.append(value)

        # Build INSERT
        qualified_table = f"{self.schema}.{table_name}"
        placeholders = ", ".join(["?" for _ in field_names])
        columns = ", ".join(field_names)

        sql = f"INSERT INTO {qualified_table} ({columns}) VALUES ({placeholders})"
        self.conn.execute(sql, values)

    def count(self, entity_type: type[Entity], **filters: Any) -> int:
        """Count entities matching filters.

        More efficient than len(list()) for large datasets.

        Args:
            entity_type: The Entity subclass to count.
            **filters: Field-value pairs to filter on.

        Returns:
            Count of matching entities.
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(filters)

        sql = f"SELECT COUNT(*) FROM {qualified_table} {where_clause}"
        result = self.conn.execute(sql, params).fetchone()

        return result[0] if result else 0

    def events(self, event_type: type[Event], **filters: Any) -> list[Event]:
        """Query the activity ledger.

        Args:
            event_type: The Event subclass to query (e.g., TicketEvent).
            **filters: Field-value pairs. Common filters:
                - entity_id="DBC-120" (all events for a ticket)
                - instance_id="abc-123" (all events by an agent)
                - since=datetime (events after a timestamp)

        Returns:
            List of matching events, ordered by created_at ascending.
        """
        table_name = self._get_table_name(event_type)
        self._ensure_table(table_name, event_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(filters)

        sql = f"SELECT * FROM {qualified_table} {where_clause} ORDER BY created_at ASC"
        results = self.conn.execute(sql, params).fetchall()

        return [self._row_to_entity(event_type, row) for row in results]

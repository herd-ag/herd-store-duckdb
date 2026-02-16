"""DuckDB implementation of StoreAdapter protocol."""

from __future__ import annotations

import dataclasses
import enum
import os
import typing
from datetime import datetime, timezone
from typing import Any

import duckdb
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
    TicketRecord: "ticket_def",
    PRRecord: "pr_def",
    DecisionRecord: "decision_record",
    ReviewRecord: "review_def",
    ModelRecord: "model_def",
    SprintRecord: "sprint_def",
}

EVENT_TABLE_MAP: dict[type[Event], str] = {
    LifecycleEvent: "agent_instance_lifecycle_activity",
    TicketEvent: "agent_instance_ticket_activity",
    PREvent: "agent_instance_pr_activity",
    ReviewEvent: "agent_instance_review_activity",
    TokenEvent: "agent_instance_token_activity",
}

# Column mapping: Entity/Event field name → DuckDB column name
# Only non-matching fields need to be listed; fields with identical names are auto-matched.
COLUMN_MAP: dict[type[Entity] | type[Event], dict[str, str]] = {
    # AgentRecord → agent_instance
    AgentRecord: {
        "id": "agent_instance_code",
        "agent": "agent_code",
        "model": "model_code",
        "ticket_id": "ticket_code",
        "state": "agent_instance_outcome",
        "spawned_by": "spawned_by_agent_instance_code",
        "started_at": "agent_instance_started_at",
        "ended_at": "agent_instance_ended_at",
        "craft_version": "craft_version_code",
        "personality_version": "personality_version_code",
    },
    # DecisionRecord → decision_record
    DecisionRecord: {
        "id": "decision_id",
        "body": "decision",
        "decision_maker": "decided_by",
        "scope": "decision_type",
        "ticket_id": "ticket_code",
    },
    # TicketRecord → ticket_def
    TicketRecord: {
        "id": "ticket_code",
        "title": "ticket_title",
        "description": "ticket_description",
        "status": "ticket_current_status",
        "project": "project_code",
        "acceptance_criteria": "ticket_acceptance_criteria",
        "tshirt_size": "ticket_tshirt_size",
        "sprint_id": "current_sprint_code",
    },
    # PRRecord → pr_def
    PRRecord: {
        "id": "pr_code",
        "ticket_id": "ticket_code",
        "title": "pr_title",
        "branch": "pr_branch_name",
        "creator_instance_id": "creator_agent_instance_code",
        "lines_added": "pr_lines_added",
        "lines_deleted": "pr_lines_deleted",
        "files_changed": "pr_files_changed",
        "merged_at": "pr_merged_at",
        "closed_at": "pr_closed_at",
    },
    # ReviewRecord → review_def
    ReviewRecord: {
        "id": "review_code",
        "pr_id": "pr_code",
        "reviewer_instance_id": "reviewer_agent_instance_code",
        "verdict": "review_verdict",
        "round": "review_round",
        "duration_minutes": "review_duration_minutes",
    },
    # ModelRecord → model_def
    ModelRecord: {
        "id": "model_code",
        "name": "model_code",
        "provider": "model_provider",
        "context_window": "model_context_window",
        "input_cost_per_token": "model_input_cost_per_m",
        "output_cost_per_token": "model_output_cost_per_m",
        "cache_read_cost_per_m": "model_cache_read_cost_per_m",
        "cache_create_cost_per_m": "model_cache_create_cost_per_m",
    },
    # SprintRecord → sprint_def
    SprintRecord: {
        "id": "sprint_code",
        "name": "sprint_title",
        "goal": "sprint_goal",
        "started_at": "sprint_started_at",
        "ended_at": "sprint_actual_end_at",
        "planned_end_at": "sprint_planned_end_at",
    },
    # LifecycleEvent → agent_instance_lifecycle_activity
    LifecycleEvent: {
        "entity_id": "agent_instance_code",
        "event_type": "lifecycle_event_type",
        "instance_id": "agent_instance_code",
        "detail": "lifecycle_detail",
    },
    # TicketEvent → agent_instance_ticket_activity
    TicketEvent: {
        "entity_id": "ticket_code",
        "event_type": "ticket_event_type",
        "instance_id": "agent_instance_code",
        "previous_status": "ticket_status",
        "new_status": "ticket_status",
        "note": "ticket_activity_comment",
        "sprint_id": "sprint_code",
        "blocker_ticket_id": "blocker_ticket_code",
        "handoff_to_agent": "handoff_to_agent_code",
    },
    # PREvent → agent_instance_pr_activity
    PREvent: {
        "entity_id": "pr_code",
        "event_type": "pr_event_type",
        "instance_id": "agent_instance_code",
        "pr_id": "pr_code",
        "commit_hash": "pr_commit_hash",
        "lines_added": "pr_push_lines_added",
        "lines_deleted": "pr_push_lines_deleted",
        "detail": "pr_activity_detail",
    },
    # ReviewEvent → agent_instance_review_activity
    ReviewEvent: {
        "entity_id": "pr_code",
        "event_type": "review_event_type",
        "instance_id": "agent_instance_code",
        "review_id": "review_code",
        "pr_id": "pr_code",
        "detail": "review_activity_detail",
        "finding_id": "review_finding_code",
    },
    # TokenEvent → agent_instance_token_activity
    TokenEvent: {
        "entity_id": "agent_instance_code",
        "instance_id": "agent_instance_code",
        "model": "model_code",
        "input_tokens": "token_input_count",
        "output_tokens": "token_output_count",
        "cost_usd": "token_cost_usd",
        "context_utilization": "token_context_utilization_pct",
        "cache_read_tokens": "token_cache_read_count",
        "cache_create_tokens": "token_cache_create_count",
    },
}


def _resolve_enum(hint: Any) -> type[enum.Enum] | None:
    """Resolve a type hint to an Enum subclass, if it is one.

    Handles plain enum types and Optional[EnumType] (i.e. EnumType | None).

    Returns:
        The Enum subclass, or None if the hint is not an enum type.
    """
    if isinstance(hint, type) and issubclass(hint, enum.Enum):
        return hint
    # Handle Union / Optional types (e.g. AgentState | None)
    for arg in typing.get_args(hint):
        if isinstance(arg, type) and issubclass(arg, enum.Enum):
            return arg
    return None


def _is_int_valued_enum(enum_type: type[enum.Enum]) -> bool:
    """Check if an enum has integer values.

    Returns True for IntEnum subclasses, or for plain Enums whose
    member values are all ints (e.g. TicketPriority with values 0-4).
    """
    if issubclass(enum_type, int):
        return True
    # Check if all member values are integers
    return all(isinstance(m.value, int) for m in enum_type)


def _get_column_name(entity_type: type[Entity] | type[Event], field_name: str) -> str:
    """Map an Entity/Event field name to its DuckDB column name.

    Args:
        entity_type: The Entity or Event type.
        field_name: The field name from the dataclass.

    Returns:
        The column name in DuckDB. Defaults to field_name if no mapping exists.
    """
    mapping = COLUMN_MAP.get(entity_type, {})
    return mapping.get(field_name, field_name)


def _get_field_names_for_column(
    entity_type: type[Entity] | type[Event], column_name: str
) -> list[str]:
    """Map a DuckDB column name to ALL Entity/Event field names that use it.

    When multiple fields map to the same column (e.g. ModelRecord.id and
    ModelRecord.name both use model_code), this returns all such fields.

    Args:
        entity_type: The Entity or Event type.
        column_name: The column name from DuckDB.

    Returns:
        List of field names. If column has no explicit mapping, returns [column_name].
    """
    mapping = COLUMN_MAP.get(entity_type, {})
    # Find all fields that map to this column
    fields = [field for field, col in mapping.items() if col == column_name]
    # If no explicit mapping, assume column_name == field_name
    return fields if fields else [column_name]


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

        # Cache resolved type hints per entity type
        self._type_hints_cache: dict[type, dict[str, Any]] = {}

    def _get_type_hints(
        self, entity_type: type[Entity] | type[Event]
    ) -> dict[str, Any]:
        """Get resolved type hints for an entity type, with caching.

        Uses typing.get_type_hints() to resolve string annotations from
        ``from __future__ import annotations`` into actual types.
        """
        if entity_type not in self._type_hints_cache:
            self._type_hints_cache[entity_type] = typing.get_type_hints(entity_type)
        return self._type_hints_cache[entity_type]

    def _ensure_table(
        self, table_name: str, entity_type: type[Entity] | type[Event]
    ) -> None:
        """Create table if it doesn't exist, or add missing columns if it does.

        Args:
            table_name: The table name (without schema).
            entity_type: The Entity or Event type to create the table for.
        """
        if table_name in self._initialized_tables:
            return

        qualified_table = f"{self.schema}.{table_name}"

        # Check if table already exists
        result = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [self.schema, table_name],
        ).fetchone()
        table_exists = result is not None and result[0] > 0

        if table_exists:
            # Table exists — ensure any new dataclass fields are added as columns
            self._ensure_columns(table_name, entity_type)
        else:
            # Get all fields from the dataclass
            fields = dataclasses.fields(entity_type)
            hints = self._get_type_hints(entity_type)

            # Build column definitions, deduplicating column names
            # (multiple Entity fields may map to the same DuckDB column)
            columns_dict: dict[str, str] = {}
            for field in fields:
                col_name = _get_column_name(entity_type, field.name)
                col_type = self._get_sql_type(field.name, hints)

                # Skip if we've already added this column name
                if col_name in columns_dict:
                    continue

                # Add PRIMARY KEY constraint to id field for entities
                if field.name == "id" and issubclass(entity_type, Entity):
                    columns_dict[col_name] = f"{col_name} {col_type} PRIMARY KEY"
                else:
                    columns_dict[col_name] = f"{col_name} {col_type}"

            columns_sql = ",\n    ".join(columns_dict.values())

            sql = f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                {columns_sql}
            )
            """
            self.conn.execute(sql)

        self._initialized_tables.add(table_name)

    def _ensure_columns(
        self, table_name: str, entity_type: type[Entity] | type[Event]
    ) -> None:
        """Add any missing columns to an existing table.

        When Entity or Event types gain new fields (e.g. HDR-0034 adding
        org/team/repo/host), existing databases won't have those columns.
        This method introspects the dataclass fields and adds any that are
        missing using ALTER TABLE ADD COLUMN IF NOT EXISTS.

        Args:
            table_name: The table name (without schema).
            entity_type: The Entity or Event type whose fields define the expected schema.
        """
        qualified_table = f"{self.schema}.{table_name}"
        fields = dataclasses.fields(entity_type)
        hints = self._get_type_hints(entity_type)

        # Deduplicate column names before adding
        seen_columns: set[str] = set()
        for field in fields:
            col_name = _get_column_name(entity_type, field.name)
            if col_name in seen_columns:
                continue
            seen_columns.add(col_name)

            col_type = self._get_sql_type(field.name, hints)
            self.conn.execute(
                f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            )

    def _get_sql_type(self, field_name: str, hints: dict[str, Any]) -> str:
        """Map a resolved Python type hint to a DuckDB SQL type.

        Args:
            field_name: The field name to look up in hints.
            hints: Resolved type hints dict from typing.get_type_hints().

        Returns:
            SQL type string.
        """
        hint = hints.get(field_name)
        if hint is None:
            return "VARCHAR"

        # Check for enum types first (handles plain and Optional enums)
        enum_type = _resolve_enum(hint)
        if enum_type is not None:
            # Integer-valued enums (IntEnum, or plain Enum with int values) -> INTEGER
            if _is_int_valued_enum(enum_type):
                return "INTEGER"
            return "VARCHAR"

        # Flatten Optional/Union to get the core type(s)
        args = typing.get_args(hint)
        core_types = [a for a in args if a is not type(None)] if args else [hint]

        for t in core_types:
            if t is datetime or (isinstance(t, type) and issubclass(t, datetime)):
                return "TIMESTAMP"

            # Handle string representation for forward refs
            type_str = str(t)

            if "Decimal" in type_str:
                return "DECIMAL(18,10)"

            if "list" in type_str.lower():
                return "VARCHAR[]"

            if t is int or (isinstance(t, type) and issubclass(t, int)):
                return "INTEGER"

            if t is float or (isinstance(t, type) and issubclass(t, float)):
                return "DOUBLE"

            if t is bool or (isinstance(t, type) and issubclass(t, bool)):
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

    def _build_where_clause(
        self, entity_type: type[Entity] | type[Event], filters: dict[str, Any]
    ) -> tuple[str, list[Any]]:
        """Build WHERE clause from filters, mapping field names to column names.

        Args:
            entity_type: The Entity or Event type (for field mapping).
            filters: Filter dictionary with Entity field names.

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
                # Map field name to column name
                col_name = _get_column_name(entity_type, key)
                # Handle enum values
                if hasattr(value, "value"):
                    value = value.value
                conditions.append(f"{col_name} = ?")
                params.append(value)

        where_clause = " AND ".join(conditions)
        return f"WHERE {where_clause}" if where_clause else "", params

    def _row_to_entity(
        self,
        entity_type: type[Entity] | type[Event],
        row: tuple,
        column_names: list[str],
    ) -> Entity | Event:
        """Convert a database row to an entity instance.

        Args:
            entity_type: The Entity or Event type.
            row: Database row tuple.
            column_names: List of column names in the same order as row values.

        Returns:
            Entity or Event instance.
        """
        hints = self._get_type_hints(entity_type)

        # Build kwargs dict from row, mapping column names back to field names
        # Handle many-to-one mappings where multiple fields use the same column
        kwargs = {}
        for i, col_name in enumerate(column_names):
            field_names = _get_field_names_for_column(entity_type, col_name)
            value = row[i]

            # Set all fields that map to this column
            for field_name in field_names:
                # Handle enum conversion using resolved type hints
                converted_value = value
                if converted_value is not None:
                    enum_type = _resolve_enum(hints.get(field_name))
                    if enum_type is not None:
                        converted_value = enum_type(converted_value)

                kwargs[field_name] = converted_value

        # Fill in any missing fields with None or default values
        for field in dataclasses.fields(entity_type):
            if field.name not in kwargs:
                if field.default is not dataclasses.MISSING:
                    kwargs[field.name] = field.default
                elif field.default_factory is not dataclasses.MISSING:
                    kwargs[field.name] = field.default_factory()
                else:
                    kwargs[field.name] = None

        return entity_type(**kwargs)

    def get(self, entity_type: type[Entity], id: str) -> Entity | None:
        """Retrieve a single entity by ID.

        Always filters out soft-deleted entities (``deleted_at IS NULL``).
        To include soft-deleted records, use ``list()`` without the
        ``active=True`` filter.

        Args:
            entity_type: The Entity subclass to retrieve (e.g., AgentRecord).
            id: The entity's unique identifier.

        Returns:
            The entity instance, or None if not found (or soft-deleted).
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        id_col = _get_column_name(entity_type, "id")
        sql = (
            f"SELECT * FROM {qualified_table} WHERE {id_col} = ? AND deleted_at IS NULL"
        )

        result = self.conn.execute(sql, [id]).fetchone()
        if result is None:
            return None

        # Get column names from cursor description
        column_names = [desc[0] for desc in self.conn.description]
        return self._row_to_entity(entity_type, result, column_names)

    def list(self, entity_type: type[Entity], **filters: Any) -> list[Entity]:
        """List entities matching filters.

        Note on soft-delete behavior: unlike ``get()``, which always excludes
        soft-deleted entities, ``list()`` returns **all** records (including
        soft-deleted ones) by default. To exclude soft-deleted entities, pass
        ``active=True`` explicitly. This is intentional — ``list()`` is the
        only way to inspect soft-deleted records, which is necessary for
        auditing and diagnostics.

        Args:
            entity_type: The Entity subclass to list (e.g., TicketRecord).
            **filters: Field-value pairs to filter on (using Entity field names).

        Returns:
            List of matching entities. Empty list if none match.
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(entity_type, filters)

        sql = f"SELECT * FROM {qualified_table} {where_clause}"
        results = self.conn.execute(sql, params).fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in self.conn.description]
        return [self._row_to_entity(entity_type, row, column_names) for row in results]

    def save(self, record: Entity) -> str:
        """Save an entity (insert or update).

        If the entity's id exists in the store, updates it.
        If not, inserts it. Sets created_at on first save and
        modified_at automatically on every save.

        Args:
            record: The entity to persist.

        Returns:
            The entity's id.
        """
        entity_type = type(record)
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        # Set created_at on first save
        if hasattr(record, "created_at") and record.created_at is None:
            record.created_at = datetime.now(timezone.utc)

        # Set modified_at
        if hasattr(record, "modified_at"):
            record.modified_at = datetime.now(timezone.utc)

        # Get all fields
        fields = dataclasses.fields(entity_type)

        # Build column names and values lists, deduplicating columns
        # If multiple fields map to the same column, keep the first (identity fields
        # come first in dataclass ordering and take priority over aliases)
        columns_dict: dict[str, Any] = {}
        for field in fields:
            col_name = _get_column_name(entity_type, field.name)
            if col_name in columns_dict:
                continue
            value = getattr(record, field.name)
            # Convert enum to value
            if hasattr(value, "value"):
                value = value.value
            columns_dict[col_name] = value

        # Build INSERT OR REPLACE
        qualified_table = f"{self.schema}.{table_name}"
        columns = list(columns_dict.keys())
        values = list(columns_dict.values())
        placeholders = ", ".join(["?" for _ in columns])
        columns_sql = ", ".join(columns)

        sql = f"INSERT OR REPLACE INTO {qualified_table} ({columns_sql}) VALUES ({placeholders})"
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
        id_col = _get_column_name(entity_type, "id")

        sql = f"UPDATE {qualified_table} SET deleted_at = ? WHERE {id_col} = ?"
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

        # Build column names and values lists, deduplicating columns
        # If multiple fields map to the same column, keep the first (identity fields
        # come first in dataclass ordering and take priority over aliases)
        columns_dict: dict[str, Any] = {}
        for field in fields:
            col_name = _get_column_name(event_type, field.name)
            if col_name in columns_dict:
                continue
            value = getattr(event, field.name)
            # Convert enum to value
            if hasattr(value, "value"):
                value = value.value
            columns_dict[col_name] = value

        # Build INSERT
        qualified_table = f"{self.schema}.{table_name}"
        columns = list(columns_dict.keys())
        values = list(columns_dict.values())
        placeholders = ", ".join(["?" for _ in columns])
        columns_sql = ", ".join(columns)

        sql = f"INSERT INTO {qualified_table} ({columns_sql}) VALUES ({placeholders})"
        self.conn.execute(sql, values)

    def count(self, entity_type: type[Entity], **filters: Any) -> int:
        """Count entities matching filters.

        More efficient than len(list()) for large datasets.

        Args:
            entity_type: The Entity subclass to count.
            **filters: Field-value pairs to filter on (using Entity field names).

        Returns:
            Count of matching entities.
        """
        table_name = self._get_table_name(entity_type)
        self._ensure_table(table_name, entity_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(entity_type, filters)

        sql = f"SELECT COUNT(*) FROM {qualified_table} {where_clause}"
        result = self.conn.execute(sql, params).fetchone()

        return result[0] if result else 0

    def events(self, event_type: type[Event], **filters: Any) -> list[Event]:
        """Query the activity ledger.

        Args:
            event_type: The Event subclass to query (e.g., TicketEvent).
            **filters: Field-value pairs (using Event field names). Common filters:
                - entity_id="DBC-145" (all events for a ticket)
                - instance_id="abc-123" (all events by an agent)
                - since=datetime (events after a timestamp)

        Returns:
            List of matching events, ordered by created_at ascending.
        """
        table_name = self._get_table_name(event_type)
        self._ensure_table(table_name, event_type)

        qualified_table = f"{self.schema}.{table_name}"
        where_clause, params = self._build_where_clause(event_type, filters)

        sql = f"SELECT * FROM {qualified_table} {where_clause} ORDER BY created_at ASC"
        results = self.conn.execute(sql, params).fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in self.conn.description]
        return [self._row_to_entity(event_type, row, column_names) for row in results]

    def storage_info(self) -> dict[str, str | int]:
        """Return storage metadata for this adapter.

        Provides information about the storage backend's location, size,
        and last modification time. Used for health checks and monitoring.

        Returns:
            Dict with keys:
                - path: Storage location (file path, directory, or empty for in-memory/cloud)
                - size_bytes: Total storage size in bytes (0 for in-memory/cloud)
                - last_modified: ISO 8601 UTC timestamp of last modification (empty for in-memory/cloud)
        """
        # Handle in-memory databases
        if self.path == ":memory:":
            return {
                "path": ":memory:",
                "size_bytes": 0,
                "last_modified": "",
            }

        # Handle file-based databases
        try:
            stat = os.stat(self.path)
            return {
                "path": self.path,
                "size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
            }
        except FileNotFoundError:
            # Database file doesn't exist yet (will be created on first write)
            return {
                "path": self.path,
                "size_bytes": 0,
                "last_modified": "",
            }

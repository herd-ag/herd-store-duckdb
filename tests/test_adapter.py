"""Tests for DuckDBStoreAdapter."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from herd_core.adapters.store import StoreAdapter
from herd_core.types import (
    AgentRecord,
    AgentState,
    DecisionRecord,
    LifecycleEvent,
    ModelRecord,
    PREvent,
    PRRecord,
    ReviewEvent,
    ReviewRecord,
    SprintRecord,
    TicketEvent,
    TicketPriority,
    TicketRecord,
    TokenEvent,
)

from herd_store_duckdb import DuckDBStoreAdapter


@pytest.fixture
def store() -> DuckDBStoreAdapter:
    """Create an in-memory DuckDB store for testing."""
    return DuckDBStoreAdapter(":memory:")


def test_isinstance_check(store: DuckDBStoreAdapter) -> None:
    """Test that adapter passes isinstance check for StoreAdapter protocol."""
    assert isinstance(store, StoreAdapter)


def test_save_and_get_agent(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving an AgentRecord."""
    agent = AgentRecord(
        id="abc-123",
        agent="grunt",
        model="claude-sonnet-4-5",
        ticket_id="DBC-145",
        state=AgentState.RUNNING,
        worktree="/tmp/worktree",
        branch="feature/test",
    )

    # Save
    returned_id = store.save(agent)
    assert returned_id == "abc-123"

    # Retrieve
    retrieved = store.get(AgentRecord, "abc-123")
    assert retrieved is not None
    assert retrieved.id == "abc-123"
    assert retrieved.agent == "grunt"
    assert retrieved.model == "claude-sonnet-4-5"
    assert retrieved.ticket_id == "DBC-145"
    assert retrieved.state == AgentState.RUNNING
    assert retrieved.worktree == "/tmp/worktree"
    assert retrieved.branch == "feature/test"
    assert retrieved.modified_at is not None


def test_get_nonexistent(store: DuckDBStoreAdapter) -> None:
    """Test getting a non-existent entity returns None."""
    result = store.get(AgentRecord, "does-not-exist")
    assert result is None


def test_save_updates_existing(store: DuckDBStoreAdapter) -> None:
    """Test that saving an existing entity updates it."""
    agent = AgentRecord(
        id="abc-123",
        agent="grunt",
        state=AgentState.SPAWNING,
    )
    store.save(agent)

    # Update
    agent.state = AgentState.RUNNING
    store.save(agent)

    # Retrieve
    retrieved = store.get(AgentRecord, "abc-123")
    assert retrieved is not None
    assert retrieved.state == AgentState.RUNNING


def test_list_with_filters(store: DuckDBStoreAdapter) -> None:
    """Test listing entities with filters."""
    # Save multiple agents
    agent1 = AgentRecord(id="agent-1", agent="grunt", state=AgentState.RUNNING)
    agent2 = AgentRecord(id="agent-2", agent="pikasso", state=AgentState.RUNNING)
    agent3 = AgentRecord(id="agent-3", agent="grunt", state=AgentState.COMPLETED)

    store.save(agent1)
    store.save(agent2)
    store.save(agent3)

    # Filter by state
    running = store.list(AgentRecord, state=AgentState.RUNNING)
    assert len(running) == 2

    # Filter by agent name
    grunts = store.list(AgentRecord, agent="grunt")
    assert len(grunts) == 2


def test_list_with_active_filter(store: DuckDBStoreAdapter) -> None:
    """Test listing with active=True filter excludes soft-deleted entities."""
    agent1 = AgentRecord(id="agent-1", agent="grunt", state=AgentState.RUNNING)
    agent2 = AgentRecord(id="agent-2", agent="pikasso", state=AgentState.RUNNING)

    store.save(agent1)
    store.save(agent2)

    # Delete agent1
    store.delete(AgentRecord, "agent-1")

    # List with active=True
    active = store.list(AgentRecord, active=True)
    assert len(active) == 1
    assert active[0].id == "agent-2"

    # List without filter shows both
    all_agents = store.list(AgentRecord)
    assert len(all_agents) == 2


def test_delete_soft_deletes(store: DuckDBStoreAdapter) -> None:
    """Test that delete soft-deletes an entity."""
    agent = AgentRecord(id="agent-1", agent="grunt")
    store.save(agent)

    # Delete
    store.delete(AgentRecord, "agent-1")

    # Get returns None for soft-deleted
    retrieved = store.get(AgentRecord, "agent-1")
    assert retrieved is None

    # But it's still in the database
    all_agents = store.list(AgentRecord)
    assert len(all_agents) == 1
    assert all_agents[0].deleted_at is not None


def test_count(store: DuckDBStoreAdapter) -> None:
    """Test counting entities."""
    agent1 = AgentRecord(id="agent-1", agent="grunt")
    agent2 = AgentRecord(id="agent-2", agent="pikasso")
    agent3 = AgentRecord(id="agent-3", agent="grunt")

    store.save(agent1)
    store.save(agent2)
    store.save(agent3)

    # Count all
    total = store.count(AgentRecord)
    assert total == 3

    # Count with filter
    grunts = store.count(AgentRecord, agent="grunt")
    assert grunts == 2


def test_count_with_active_filter(store: DuckDBStoreAdapter) -> None:
    """Test counting with active filter."""
    agent1 = AgentRecord(id="agent-1", agent="grunt")
    agent2 = AgentRecord(id="agent-2", agent="pikasso")

    store.save(agent1)
    store.save(agent2)
    store.delete(AgentRecord, "agent-1")

    # Count active
    active = store.count(AgentRecord, active=True)
    assert active == 1


def test_append_lifecycle_event(store: DuckDBStoreAdapter) -> None:
    """Test appending a lifecycle event."""
    event = LifecycleEvent(
        entity_id="DBC-145",
        event_type="spawned",
        instance_id="abc-123",
        detail="Agent spawned successfully",
    )

    # Append
    store.append(event)

    # Query back
    events = store.events(LifecycleEvent, entity_id="DBC-145")
    assert len(events) == 1
    assert events[0].entity_id == "DBC-145"
    assert events[0].event_type == "spawned"
    assert events[0].instance_id == "abc-123"
    assert events[0].detail == "Agent spawned successfully"
    assert events[0].created_at is not None


def test_append_sets_created_at(store: DuckDBStoreAdapter) -> None:
    """Test that append sets created_at if not provided."""
    event = LifecycleEvent(
        entity_id="DBC-145",
        event_type="spawned",
        instance_id="abc-123",
        detail="Test",
    )

    store.append(event)

    events = store.events(LifecycleEvent, entity_id="DBC-145")
    assert events[0].created_at is not None


def test_events_with_since_filter(store: DuckDBStoreAdapter) -> None:
    """Test querying events with since filter."""
    now = datetime.now(timezone.utc)

    # Append events with different timestamps
    event1 = LifecycleEvent(
        entity_id="DBC-145",
        event_type="spawned",
        instance_id="abc-123",
        detail="Event 1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    event2 = LifecycleEvent(
        entity_id="DBC-145",
        event_type="started",
        instance_id="abc-123",
        detail="Event 2",
        created_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )

    store.append(event1)
    store.append(event2)

    # Query with since
    events = store.events(
        LifecycleEvent, since=datetime(2026, 1, 15, tzinfo=timezone.utc)
    )
    assert len(events) == 1
    assert events[0].detail == "Event 2"


def test_events_ordered_by_created_at(store: DuckDBStoreAdapter) -> None:
    """Test that events are ordered by created_at ascending."""
    event1 = LifecycleEvent(
        entity_id="DBC-145",
        event_type="spawned",
        instance_id="abc-123",
        detail="Event 1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    event2 = LifecycleEvent(
        entity_id="DBC-145",
        event_type="started",
        instance_id="abc-123",
        detail="Event 2",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    store.append(event1)
    store.append(event2)

    events = store.events(LifecycleEvent, entity_id="DBC-145")
    assert len(events) == 2
    assert events[0].detail == "Event 2"
    assert events[1].detail == "Event 1"


def test_multiple_entity_types(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving multiple entity types."""
    # Save agent
    agent = AgentRecord(id="agent-1", agent="grunt")
    store.save(agent)

    # Save ticket
    ticket = TicketRecord(
        id="DBC-145",
        title="Test ticket",
        status="in_progress",
        priority=TicketPriority.HIGH,
    )
    store.save(ticket)

    # Save PR
    pr = PRRecord(
        id="pr-1",
        ticket_id="DBC-145",
        title="Test PR",
        branch="feature/test",
        status="open",
    )
    store.save(pr)

    # Retrieve
    retrieved_agent = store.get(AgentRecord, "agent-1")
    retrieved_ticket = store.get(TicketRecord, "DBC-145")
    retrieved_pr = store.get(PRRecord, "pr-1")

    assert retrieved_agent is not None
    assert retrieved_ticket is not None
    assert retrieved_pr is not None

    assert retrieved_agent.agent == "grunt"
    assert retrieved_ticket.title == "Test ticket"
    assert retrieved_ticket.priority == TicketPriority.HIGH
    assert retrieved_pr.title == "Test PR"


def test_multiple_event_types(store: DuckDBStoreAdapter) -> None:
    """Test appending and querying multiple event types."""
    # Append lifecycle event
    lifecycle_event = LifecycleEvent(
        entity_id="DBC-145",
        event_type="spawned",
        instance_id="abc-123",
        detail="Spawned",
    )
    store.append(lifecycle_event)

    # Append ticket event
    ticket_event = TicketEvent(
        entity_id="DBC-145",
        event_type="status_change",
        instance_id="abc-123",
        previous_status="backlog",
        new_status="in_progress",
    )
    store.append(ticket_event)

    # Append PR event
    pr_event = PREvent(
        entity_id="DBC-145",
        event_type="created",
        instance_id="abc-123",
        pr_id="pr-1",
        detail="PR created",
    )
    store.append(pr_event)

    # Query each type
    lifecycle_events = store.events(LifecycleEvent, entity_id="DBC-145")
    ticket_events = store.events(TicketEvent, entity_id="DBC-145")
    pr_events = store.events(PREvent, entity_id="DBC-145")

    assert len(lifecycle_events) == 1
    assert len(ticket_events) == 1
    assert len(pr_events) == 1


def test_decision_record(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving DecisionRecord."""
    decision = DecisionRecord(
        id="hdr-001",
        title="Use DuckDB for storage",
        body="DuckDB is lightweight and embedded",
        decision_maker="mini-mao",
        principle="simplicity",
        scope="storage",
        status="accepted",
    )
    store.save(decision)

    retrieved = store.get(DecisionRecord, "hdr-001")
    assert retrieved is not None
    assert retrieved.title == "Use DuckDB for storage"
    assert retrieved.decision_maker == "mini-mao"


def test_review_record(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving ReviewRecord."""
    review = ReviewRecord(
        id="review-1",
        pr_id="pr-1",
        ticket_id="DBC-145",
        reviewer_instance_id="wardenstein-1",
        verdict="pass",
        body="Looks good",
        findings_count=0,
    )
    store.save(review)

    retrieved = store.get(ReviewRecord, "review-1")
    assert retrieved is not None
    assert retrieved.pr_id == "pr-1"
    assert retrieved.verdict == "pass"


def test_model_record(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving ModelRecord."""
    model = ModelRecord(
        id="model-1",
        name="claude-sonnet-4-5",
        provider="anthropic",
        input_cost_per_token=Decimal("0.000003"),
        output_cost_per_token=Decimal("0.000015"),
        context_window=200000,
        target_role="grunt",
    )
    store.save(model)

    retrieved = store.get(ModelRecord, "model-1")
    assert retrieved is not None
    assert retrieved.name == "claude-sonnet-4-5"
    assert retrieved.provider == "anthropic"
    assert retrieved.input_cost_per_token == Decimal("0.000003")


def test_sprint_record(store: DuckDBStoreAdapter) -> None:
    """Test saving and retrieving SprintRecord."""
    sprint = SprintRecord(
        id="sprint-1",
        name="Sprint 1",
        number=1,
        status="active",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        goal="Build the foundation",
    )
    store.save(sprint)

    retrieved = store.get(SprintRecord, "sprint-1")
    assert retrieved is not None
    assert retrieved.name == "Sprint 1"
    assert retrieved.number == 1


def test_token_event(store: DuckDBStoreAdapter) -> None:
    """Test appending and retrieving TokenEvent."""
    token_event = TokenEvent(
        entity_id="DBC-145",
        event_type="usage",
        instance_id="abc-123",
        model="claude-sonnet-4-5",
        input_tokens=1000,
        output_tokens=500,
        total_tokens=1500,
        cost_usd=Decimal("0.0225"),
        context_utilization=Decimal("0.0075"),
    )
    store.append(token_event)

    events = store.events(TokenEvent, instance_id="abc-123")
    assert len(events) == 1
    assert events[0].model == "claude-sonnet-4-5"
    assert events[0].total_tokens == 1500
    assert events[0].cost_usd == Decimal("0.0225")


def test_review_event(store: DuckDBStoreAdapter) -> None:
    """Test appending and retrieving ReviewEvent."""
    review_event = ReviewEvent(
        entity_id="DBC-145",
        event_type="reviewed",
        instance_id="wardenstein-1",
        review_id="review-1",
        pr_id="pr-1",
        verdict="pass",
        detail="No issues found",
    )
    store.append(review_event)

    events = store.events(ReviewEvent, pr_id="pr-1")
    assert len(events) == 1
    assert events[0].verdict == "pass"

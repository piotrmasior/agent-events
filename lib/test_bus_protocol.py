"""Tests for bus_protocol helpers. No live bus needed."""

import json
import time
import uuid
from pathlib import Path

import pytest

from bus_protocol import (
    emit_event,
    is_eligible_for,
    load_manifest,
    read_thread,
    recent_events,
    validate_event,
)


def _make_event(tmp_path: Path, payload_type: str = "task.request", capabilities=None):
    events_dir = tmp_path / "events"
    events_dir.mkdir(exist_ok=True)
    event = emit_event(
        events_dir,
        type=payload_type,
        thread_id=str(uuid.uuid4()),
        source={"agent": "test", "machine": "macstudio", "session_id": None},
        payload={
            "task": "dummy",
            "capabilities": capabilities or [],
            "context": {},
            "deadline": "2030-01-01T00:00:00Z",
            "priority": "normal",
            "retry_count": 0,
            "reply_to_event_id": "x",
        },
    )
    return events_dir, event


def test_emit_event_writes_valid_json(tmp_path: Path):
    events_dir, event = _make_event(tmp_path)
    files = list(events_dir.glob("*.json"))
    assert len(files) == 1
    loaded = json.loads(files[0].read_text())
    assert loaded["id"] == event["id"]
    assert loaded["type"] == "task.request"


def test_validate_event_rejects_bad_type():
    with pytest.raises(ValueError):
        validate_event(
            {
                "id": "x",
                "type": "bogus",
                "thread_id": "t",
                "source": {"agent": "a", "machine": "m"},
                "created_at": "2026-01-01T00:00:00Z",
                "payload": {},
            }
        )


def test_is_eligible_for_matches_subset():
    manifest = {"capabilities": ["code-review", "planning", "local-compute:macstudio"]}
    assert is_eligible_for(["planning"], manifest) is True
    assert is_eligible_for(["code-review", "planning"], manifest) is True
    assert is_eligible_for(["web-research"], manifest) is False


def test_is_eligible_for_empty_caps_always_eligible():
    manifest = {"capabilities": []}
    assert is_eligible_for([], manifest) is True


def test_recent_events_filters_by_type(tmp_path: Path):
    events_dir, _ = _make_event(tmp_path, payload_type="task.request")
    _make_event(tmp_path, payload_type="heartbeat")
    reqs = list(recent_events(events_dir, filter_types={"task.request"}))
    assert len(reqs) == 1
    assert reqs[0]["type"] == "task.request"


def test_read_thread_collects_by_thread_id(tmp_path: Path):
    events_dir, first = _make_event(tmp_path)
    time.sleep(0.002)
    emit_event(
        events_dir,
        type="task.claim",
        thread_id=first["thread_id"],
        source={"agent": "test", "machine": "macstudio", "session_id": None},
        payload={"claiming_task_id": first["id"], "lease_duration_s": 90, "expected_completion_s": 120},
    )
    thread = read_thread(events_dir, first["thread_id"])
    assert len(thread) == 2
    assert thread[0]["id"] == first["id"]


def test_load_manifest(tmp_path: Path):
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    (manifests / "codex.json").write_text(
        json.dumps(
            {
                "agent": "codex",
                "machine": "macstudio",
                "capabilities": ["planning"],
                "jitter_ms": 100,
            }
        )
    )
    manifest = load_manifest(manifests, "codex")
    assert manifest["agent"] == "codex"
    assert manifest["capabilities"] == ["planning"]

"""Shared helpers for mesh event bus adapters.

Canonical directories:
- events:    ~/agent-events/events/
- manifests: ~/agent-events/manifests/
- state:     ~/agent-events/state/
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

VALID_EVENT_TYPES = {
    "task.request",
    "task.claim",
    "task.progress",
    "task.result",
    "task.fail",
    "task.reassign",
    "heartbeat",
    "agent.recovered",
}


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validate_event(event: dict) -> None:
    required = ("id", "type", "thread_id", "source", "created_at", "payload")
    for key in required:
        if key not in event:
            raise ValueError(f"missing required field: {key}")

    if event["type"] not in VALID_EVENT_TYPES:
        raise ValueError(f"unknown event type: {event['type']}")

    source = event["source"]
    if not isinstance(source, dict):
        raise ValueError("source must be an object")
    if "agent" not in source or "machine" not in source:
        raise ValueError("source must include agent and machine")



def _event_files(events_dir: Path) -> list[Path]:
    if not events_dir.exists():
        return []
    files = [
        path for path in events_dir.rglob("*.json")
        if path.is_file()
    ]
    files.sort(key=lambda p: (p.stat().st_mtime, p.name))
    return files



def emit_event(events_dir: Path, *, type: str, thread_id: str, source: dict, payload: dict) -> dict:
    events_dir = Path(events_dir)
    events_dir.mkdir(parents=True, exist_ok=True)

    event = {
        "id": str(uuid.uuid4()),
        "type": type,
        "thread_id": thread_id,
        "source": source,
        "created_at": utc_iso_now(),
        "payload": payload,
    }
    validate_event(event)

    epoch_ms = int(time.time() * 1000)
    agent = str(source.get("agent", "unknown")).replace("/", "-")
    event_type = type.replace(".", "_")
    fname = f"{epoch_ms}-{agent}-{event_type}-{event['id'][:8]}.json"
    target = events_dir / fname
    target.write_text(json.dumps(event, indent=2) + "\n")
    return event



def recent_events(
    events_dir: Path,
    filter_types: Optional[set[str]] = None,
    since_mtime: Optional[float] = None,
) -> Iterator[dict]:
    for file_path in _event_files(Path(events_dir)):
        mtime = file_path.stat().st_mtime
        if since_mtime is not None and mtime < since_mtime:
            continue
        try:
            event = json.loads(file_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if filter_types and event.get("type") not in filter_types:
            continue
        yield event



def read_thread(events_dir: Path, thread_id: str) -> list[dict]:
    events = [event for event in recent_events(events_dir) if event.get("thread_id") == thread_id]
    events.sort(key=lambda e: (e.get("created_at", ""), e.get("id", "")))
    return events



def is_eligible_for(required_caps: list[str], manifest: dict) -> bool:
    return set(required_caps).issubset(set(manifest.get("capabilities", [])))



def load_manifest(manifests_dir: Path, agent: str) -> dict:
    path = Path(manifests_dir) / f"{agent}.json"
    return json.loads(path.read_text())



def events_dir() -> Path:
    return Path(os.environ.get("AGENT_EVENTS_DIR", Path.home() / "agent-events" / "events"))



def manifests_dir() -> Path:
    return Path(os.environ.get("AGENT_MANIFESTS_DIR", Path.home() / "agent-events" / "manifests"))



def state_dir() -> Path:
    path = Path(os.environ.get("AGENT_STATE_DIR", Path.home() / "agent-events" / "state"))
    path.mkdir(parents=True, exist_ok=True)
    return path

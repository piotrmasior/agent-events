#!/usr/bin/env python3
"""DynamoDB bridge for the agent event bus.

Polls DynamoDB every 5 seconds, materializes remote events as local files.
Syncs enrichment updates for events it has previously written.

Usage: python3 bridge.py [--interval 5] [--resync-hours 1]
"""

import json
import os
import sys
import time
import signal
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from eventbus_core import (
    EVENTS_DIR,
    MACHINE,
    TABLE_NAME,
    REGION,
    write_event_file,
    parse_event_file,
    enrich_event,
    dynamo_query_new,
    dynamo_query_modified,
)

BASE_DIR = Path(__file__).parent
STATE_FILE = BASE_DIR / ".bridge-state"
HEARTBEAT_FILE = BASE_DIR / ".bridge-heartbeat"
DEFAULT_INTERVAL = 5
DEFAULT_RESYNC_HOURS = 1


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"last_seen_ts": None, "seen_ids": {}, "last_modified_check": None}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)


def write_heartbeat():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    HEARTBEAT_FILE.write_text(ts)


def sync_new_events(state):
    try:
        events = dynamo_query_new(
            since_ts=state.get("last_seen_ts"),
            exclude_machine=MACHINE,
            days_back=1,
        )
    except Exception as e:
        print(f"[bridge] DynamoDB query failed: {e}", file=sys.stderr)
        return 0

    synced = 0
    for event in events:
        short_id = event.get("short_id", "")
        if not short_id:
            continue
        if short_id in state.get("seen_ids", {}):
            continue
        topic = event.get("topic", "")
        if not topic:
            continue

        try:
            path = write_event_file(
                topic=topic,
                tags=event.get("tags", []),
                source_machine=event.get("source_machine", "unknown"),
                source_agent=event.get("source_agent", "unknown"),
                message=event.get("body", ""),
                event_id=short_id,
            )

            for log_entry in event.get("log_entries", []):
                enrich_event(
                    path,
                    log_entry.split("] ", 1)[-1] if "] " in log_entry else log_entry,
                    machine=event.get("source_machine", "unknown"),
                    agent=event.get("source_agent", "unknown"),
                )

            state.setdefault("seen_ids", {})[short_id] = event.get("created", "")
            synced += 1
            print(f"[bridge] Synced: {short_id} -> {topic}")
        except Exception as e:
            print(f"[bridge] Failed to write event {short_id}: {e}", file=sys.stderr)

    if events:
        latest_ts = max(e.get("created", "") for e in events)
        if not state.get("last_seen_ts") or latest_ts > state["last_seen_ts"]:
            state["last_seen_ts"] = latest_ts

    return synced


def sync_enrichments(state):
    last_check = state.get("last_modified_check")
    if not last_check:
        state["last_modified_check"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return 0

    try:
        # Intentionally NOT excluding MACHINE here: we need enrichments to our
        # own published events (done by other machines) to sync back. The loop
        # below dedups via state.seen_ids + log_count comparison, so re-reading
        # our own enrichments is a no-op.
        modified = dynamo_query_modified(
            since_ts=last_check,
            exclude_machine=None,
        )
    except Exception as e:
        print(f"[bridge] DynamoDB modified query failed: {e}", file=sys.stderr)
        return 0

    updated = 0
    for event in modified:
        short_id = event.get("short_id", "")
        if not short_id:
            continue

        # Don't gate on seen_ids — it's only populated for remote-originated
        # events (sync_new_events filters out own machine). Enrichments to our
        # own published events would be silently dropped if we required
        # seen_ids membership. find_event_by_id below is the real existence
        # check.
        from eventbus_core import find_event_by_id
        local_path = find_event_by_id(short_id)
        if not local_path:
            continue

        local_parsed = parse_event_file(local_path)
        local_log_count = len(local_parsed.get("log", []))
        remote_log_entries = event.get("log_entries", [])

        if len(remote_log_entries) > local_log_count:
            for entry in remote_log_entries[local_log_count:]:
                parts = entry.split("] ", 1)
                msg = parts[-1] if len(parts) > 1 else entry
                source = "unknown/unknown"
                if "[" in entry and "]" in entry:
                    source = entry.split("[")[1].split("]")[0]
                machine, agent = (source.split("/", 1) + ["unknown"])[:2]
                enrich_event(local_path, msg, machine=machine, agent=agent)
            updated += 1
            print(f"[bridge] Updated enrichment: {short_id} (+{len(remote_log_entries) - local_log_count} entries)")

    state["last_modified_check"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return updated


def resync_state(state, hours=1):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    state["last_seen_ts"] = cutoff
    state["seen_ids"] = {}
    state["last_modified_check"] = cutoff
    return state


def run(interval=DEFAULT_INTERVAL, resync_hours=DEFAULT_RESYNC_HOURS):
    print(f"[bridge] Starting on {MACHINE}, polling every {interval}s")
    print(f"[bridge] Table: {TABLE_NAME} in {REGION}")
    print(f"[bridge] Events dir: {EVENTS_DIR}")

    state = load_state()
    if not state.get("last_seen_ts"):
        print(f"[bridge] No state found, re-syncing last {resync_hours}h")
        state = resync_state(state, resync_hours)

    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    state["seen_ids"] = {
        k: v for k, v in state.get("seen_ids", {}).items()
        if v > cutoff_24h
    }

    heartbeat_counter = 0

    running = True
    def handle_signal(signum, frame):
        nonlocal running
        print(f"\n[bridge] Shutting down (signal {signum})")
        save_state(state)
        running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while running:
        try:
            new = sync_new_events(state)
            updated = sync_enrichments(state)

            if new or updated:
                save_state(state)

            heartbeat_counter += 1
            if heartbeat_counter >= 2:
                write_heartbeat()
                heartbeat_counter = 0
                save_state(state)

        except Exception as e:
            print(f"[bridge] Unexpected error: {e}", file=sys.stderr)

        time.sleep(interval)

    print("[bridge] Stopped.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Agent event bus DynamoDB bridge")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Poll interval in seconds (default: 5)")
    parser.add_argument("--resync-hours", type=int, default=DEFAULT_RESYNC_HOURS, help="Hours to re-sync on lost state (default: 1)")
    args = parser.parse_args()
    run(interval=args.interval, resync_hours=args.resync_hours)

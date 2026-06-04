#!/usr/bin/env bash
# eventbus.sh — CLI for the agent event bus
# Usage: eventbus.sh <command> [args]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVENTS_DIR="${SCRIPT_DIR}/events"
ARCHIVE_DIR="${SCRIPT_DIR}/archive"

# Machine detection (same pattern as activity.sh)
if [[ -n "${ACTIVITY_MACHINE:-}" ]]; then
    MACHINE="$ACTIVITY_MACHINE"
else
    case "$(hostname 2>/dev/null)" in
        *192.168.1.102*|*Mac-Studio*|*macstudio*) MACHINE="macstudio" ;;
        *192.168.0.195*|*raspberrypi*|*pmasior-pi*|*uhf-pi*) MACHINE="pi" ;;
        *MacBook*|*macbook*) MACHINE="macbook" ;;
        *) MACHINE="unknown" ;;
    esac
fi

AGENT="${EVENTBUS_AGENT:-claude-code}"

# ── Commands ──

cmd_publish() {
    local topic="" message="" tags="" project=""
    local positional=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tag)  tags="${2:?--tag requires a value}"; shift 2 ;;
            --project) project="${2:?--project requires a value}"; shift 2 ;;
            *) positional+=("$1"); shift ;;
        esac
    done

    topic="${positional[0]:?Usage: eventbus.sh publish <topic> <message> [--tag t1,t2] [--project name]}"
    message="${positional[1]:?Usage: eventbus.sh publish <topic> <message>}"

    # Validate topic has at least 2 levels
    if [[ "$(echo "$topic" | tr '/' '\n' | wc -l)" -lt 2 ]]; then
        echo "Error: topic must have at least 2 levels (e.g. project/llm-server)" >&2
        return 1
    fi

    # Call Python to write file + DynamoDB
    python3 - "$topic" "$message" "$tags" "$MACHINE" "$AGENT" "$project" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import write_event_file, dynamo_put_event, generate_event_id

topic, message, tags_str, machine, agent, project = sys.argv[1:7]
tags = [t.strip() for t in tags_str.split(",") if t.strip()] if tags_str else []
if project and project not in tags:
    tags.append(project)

event_id = generate_event_id()
path = write_event_file(topic, tags, machine, agent, message, event_id=event_id)

# DynamoDB dual-write (background, non-blocking)
import threading
t = threading.Thread(target=dynamo_put_event, args=(event_id, topic, tags, machine, agent, message), daemon=True)
t.start()

print(f"{event_id} {path}")
t.join(timeout=5)
PYEOF
}

cmd_subscribe() {
    local pattern="${1:?Usage: eventbus.sh subscribe <topic-pattern>}"
    local recursive=false
    local watch_dir

    # Handle wildcards
    if [[ "$pattern" == *"/#" || "$pattern" == *"#" ]]; then
        recursive=true
        watch_dir="${EVENTS_DIR}/${pattern%%/#}"
        watch_dir="${watch_dir%%#}"
    else
        watch_dir="${EVENTS_DIR}/${pattern}"
    fi

    # Create dir if it doesn't exist (subscribe before first publish)
    mkdir -p "$watch_dir"

    echo "[eventbus] Watching: $watch_dir (recursive=$recursive)" >&2

    if command -v fswatch &>/dev/null; then
        # macOS: use fswatch
        if $recursive; then
            fswatch --recursive --event Created --event Updated "$watch_dir" | while read -r file; do
                [[ "$file" == *.event ]] && echo "$file"
            done
        else
            fswatch --event Created --event Updated "$watch_dir" | while read -r file; do
                [[ "$file" == *.event ]] && echo "$file"
            done
        fi
    elif command -v inotifywait &>/dev/null; then
        # Linux: use inotifywait
        local inotify_flags="--format %w%f -e create -e modify -m"
        if $recursive; then
            inotify_flags="-r $inotify_flags"
        fi
        inotifywait $inotify_flags "$watch_dir" 2>/dev/null | while read -r file; do
            [[ "$file" == *.event ]] && echo "$file"
        done
    else
        echo "Error: neither fswatch nor inotifywait found. Install fswatch (macOS) or inotify-tools (Linux)." >&2
        return 1
    fi
}

cmd_list() {
    local pattern="" since=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --since) since="$2"; shift 2 ;;
            *) pattern="$1"; shift ;;
        esac
    done

    python3 - "$pattern" "$since" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import list_events, recent_events

pattern, since = sys.argv[1], sys.argv[2]
since = since if since else None

if pattern:
    events = list_events(pattern, since=since)
else:
    events = recent_events(20)

for e in events:
    acked = "+" if e.get("acknowledged") else " "
    tags = ",".join(e.get("tags", []))
    log_count = len(e.get("log", []))
    print(f"[{acked}] {e.get('id','?'):8s} | {e.get('topic',''):30s} | {e.get('source_machine',''):10s} | {e.get('created',''):20s} | logs:{log_count} | {tags}")
PYEOF
}

cmd_read() {
    local event_id="${1:?Usage: eventbus.sh read <event-id>}"

    python3 - "$event_id" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import find_event_by_id

path = find_event_by_id(sys.argv[1])
if path:
    print(path.read_text())
else:
    print(f"Event {sys.argv[1]} not found", file=sys.stderr)
    sys.exit(1)
PYEOF
}

cmd_recent() {
    local count="${1:-10}"

    python3 - "$count" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import recent_events

events = recent_events(int(sys.argv[1]))
for e in events:
    acked = "+" if e.get("acknowledged") else " "
    tags = ",".join(e.get("tags", []))
    log_count = len(e.get("log", []))
    print(f"[{acked}] {e.get('id','?'):8s} | {e.get('topic',''):30s} | {e.get('source_machine',''):10s} | {e.get('created',''):20s} | logs:{log_count} | {tags}")
PYEOF
}

cmd_enrich() {
    local event_id="${1:?Usage: eventbus.sh enrich <event-id> <message>}"
    shift
    local message="${1:?Usage: eventbus.sh enrich <event-id> <message>}"

    python3 - "$event_id" "$message" "$MACHINE" "$AGENT" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import find_event_by_id, enrich_event, dynamo_enrich_event
from datetime import datetime, timezone

event_id, message, machine, agent = sys.argv[1:5]
path = find_event_by_id(event_id)
if not path:
    print(f"Event {event_id} not found", file=sys.stderr)
    sys.exit(1)

enrich_event(path, message, machine=machine, agent=agent)

# DynamoDB enrichment (background)
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
log_entry = f"{ts} [{machine}/{agent}] {message}"
import threading
t = threading.Thread(target=dynamo_enrich_event, args=(event_id, log_entry), daemon=True)
t.start()

print(f"Enriched {event_id}")
t.join(timeout=5)
PYEOF
}

cmd_ack() {
    local event_id="${1:?Usage: eventbus.sh ack <event-id>}"

    python3 - "$event_id" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import find_event_by_id, ack_event

path = find_event_by_id(sys.argv[1])
if not path:
    print(f"Event {sys.argv[1]} not found", file=sys.stderr)
    sys.exit(1)
ack_event(path)
print(f"Acknowledged {sys.argv[1]}")
PYEOF
}

cmd_archive() {
    local pattern="" all_flag=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) all_flag=true; shift ;;
            *) pattern="$1"; shift ;;
        esac
    done

    python3 - "$pattern" "$all_flag" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import archive_topic, EVENTS_DIR
from pathlib import Path

pattern, all_flag = sys.argv[1], sys.argv[2].lower() == "true"

if all_flag:
    total = 0
    for root_dir in ["project", "agent", "machine", "user"]:
        d = EVENTS_DIR / root_dir
        if d.exists():
            for topic_dir in d.rglob("*"):
                if topic_dir.is_dir() and list(topic_dir.glob("*.event")):
                    rel = str(topic_dir.relative_to(EVENTS_DIR))
                    archived = archive_topic(rel)
                    total += len(archived)
    print(f"Archived {total} events")
elif pattern:
    archived = archive_topic(pattern)
    print(f"Archived {len(archived)} events from {pattern}")
else:
    print("Usage: eventbus.sh archive <topic-pattern> [--all]", file=sys.stderr)
    sys.exit(1)
PYEOF
}

cmd_purge() {
    local days=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --older-than)
                local value="${2:?--older-than requires a duration (e.g. 7d)}"
                days="${value%%d}"
                shift 2
                ;;
            *) shift ;;
        esac
    done

    if [[ -z "$days" ]]; then
        echo "Usage: eventbus.sh purge --older-than <N>d" >&2
        return 1
    fi

    python3 - "$days" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import purge_archive

days = int(sys.argv[1])
deleted = purge_archive(days)
print(f"Purged {deleted} archived events older than {days} days")
PYEOF
}

cmd_auto_archive() {
    local hours="${AUTO_ARCHIVE_HOURS:-6}"
    local acked_only="true"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --hours)
                hours="${2:?--hours requires a number}"
                shift 2
                ;;
            --include-unacked)
                acked_only="false"
                shift
                ;;
            *) shift ;;
        esac
    done

    python3 - "$hours" "$acked_only" <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import archive_stale

hours = float(sys.argv[1])
acked_only = sys.argv[2] == "true"
archived = archive_stale(hours, acked_only=acked_only)
if not archived:
    print(f"No stale events to archive (cutoff: {hours}h, acked_only={acked_only})")
else:
    print(f"Archived {len(archived)} stale events (cutoff: {hours}h, acked_only={acked_only})")
    for short_id, reason, _ in archived[:20]:
        print(f"  {short_id} ({reason})")
    if len(archived) > 20:
        print(f"  ... and {len(archived) - 20} more")
PYEOF
}

cmd_topics() {
    python3 - <<'PYEOF'
import sys, os
sys.path.insert(0, os.environ.get("EVENTBUS_SCRIPT_DIR", os.path.expanduser("~/agent-events")))
from eventbus_core import list_topics

topics = list_topics()
if not topics:
    print("No active events")
else:
    for topic, count in topics:
        print(f"  {topic:40s} {count} event(s)")
PYEOF
}

cmd_status() {
    echo "=== Agent Event Bus Status ==="
    echo ""

    # Bridge health
    local heartbeat_file="${SCRIPT_DIR}/.bridge-heartbeat"
    if [[ -f "$heartbeat_file" ]]; then
        local last_beat
        last_beat=$(cat "$heartbeat_file")
        local now
        now=$(date -u +%s)
        local beat_ts
        if [[ "$(uname)" == "Darwin" ]]; then
            beat_ts=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${last_beat%Z}" +%s 2>/dev/null || echo "0")
        else
            beat_ts=$(date -u -d "${last_beat%Z}" +%s 2>/dev/null || echo "0")
        fi
        local age=$(( now - beat_ts ))
        if [[ $age -lt 30 ]]; then
            echo "Bridge: HEALTHY (last heartbeat ${age}s ago)"
        else
            echo "Bridge: UNHEALTHY (last heartbeat ${age}s ago)"
        fi
    else
        echo "Bridge: NOT RUNNING (no heartbeat file)"
    fi

    echo ""
    echo "Active events:"
    cmd_topics

    echo ""
    # Archive size
    if [[ -d "$ARCHIVE_DIR" ]]; then
        local archive_count
        archive_count=$(find "$ARCHIVE_DIR" -name "*.event" 2>/dev/null | wc -l | tr -d ' ')
        local archive_size
        archive_size=$(du -sh "$ARCHIVE_DIR" 2>/dev/null | cut -f1)
        echo "Archive: ${archive_count} events (${archive_size})"
    else
        echo "Archive: empty"
    fi

    echo ""
    echo "Machine: $MACHINE"
}

# ── Help ──

cmd_help() {
    cat <<'HELP'
Agent Event Bus — eventbus.sh

Publishing:
  publish <topic> <message> [--tag t1,t2] [--project name]

Subscribing:
  subscribe <topic-pattern>     Watch for new events (use with Monitor tool)
                                Wildcards: topic/# (recursive), topic/*/sub (glob)

Reading:
  list [topic] [--since 1h]    List active events
  read <event-id>              Show full event content
  recent [N]                   Last N events across all topics (default 10)

Enrichment:
  enrich <event-id> <message>  Append timestamped log entry to event

Lifecycle:
  ack <event-id>               Mark event as acknowledged
  archive <topic> [--all]      Move events to archive/YYYY-MM-DD/
  auto-archive [--hours N]     Archive acked events older than N hours (default 6).
                               Add --include-unacked to sweep stale unacked too.
  purge --older-than <N>d      Delete archived events older than N days

Housekeeping:
  topics                       List active topic directories with counts
  status                       Bridge health, event counts, archive size
HELP
}

# ── Dispatch ──

export EVENTBUS_SCRIPT_DIR="$SCRIPT_DIR"

case "${1:-help}" in
    publish)    shift; cmd_publish "$@" ;;
    subscribe)  shift; cmd_subscribe "$@" ;;
    list)       shift; cmd_list "$@" ;;
    read)       shift; cmd_read "$@" ;;
    recent)     shift; cmd_recent "$@" ;;
    enrich)     shift; cmd_enrich "$@" ;;
    ack)        shift; cmd_ack "$@" ;;
    archive)    shift; cmd_archive "$@" ;;
    auto-archive) shift; cmd_auto_archive "$@" ;;
    purge)      shift; cmd_purge "$@" ;;
    topics)     shift; cmd_topics ;;
    status)     shift; cmd_status ;;
    help|*)     cmd_help ;;
esac

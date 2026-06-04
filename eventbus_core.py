"""Core library for the agent event bus.

Handles event file I/O, YAML frontmatter parsing, and DynamoDB operations.
"""

import os
import time
import uuid
import fcntl
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Configuration ──

BASE_DIR = Path(os.environ.get("EVENTBUS_DIR", Path.home() / "agent-events"))
EVENTS_DIR = BASE_DIR / "events"
ARCHIVE_DIR = BASE_DIR / "archive"
TABLE_NAME = os.environ.get("EVENTBUS_DYNAMO_TABLE", "agent-events")
REGION = os.environ.get(
    "EVENTBUS_DYNAMO_REGION",
    os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
)
MACHINE = os.environ.get("ACTIVITY_MACHINE", "unknown")

# ── Event ID & Filename ──


def generate_event_id():
    """Generate an 8-character unique event ID."""
    return uuid.uuid4().hex[:8]


def _make_filename(event_id):
    """Create event filename: <unix-timestamp-microseconds>-<short-id>.event"""
    ts = time.time()
    ts_micro = f"{ts:.6f}".replace(".", "")
    return f"{ts_micro}-{event_id}.event"


# ── Event File I/O ──


def write_event_file(topic, tags, source_machine, source_agent, message, event_id=None):
    """Write an event file to the topic directory. Returns the file Path."""
    if event_id is None:
        event_id = generate_event_id()
    filename = _make_filename(event_id)
    dir_path = EVENTS_DIR / topic
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / filename

    created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tags_str = ", ".join(tags) if tags else ""

    content = f"""---
id: {event_id}
topic: {topic}
tags: [{tags_str}]
source_machine: {source_machine}
source_agent: {source_agent}
created: {created}
acknowledged: false
---
{message}
"""
    file_path.write_text(content)
    return file_path


def parse_event_file(file_path):
    """Parse an event file into a dict with frontmatter fields, body, and log entries."""
    file_path = Path(file_path)
    content = file_path.read_text()

    # Split frontmatter from body
    parts = content.split("---\n", 2)
    if len(parts) < 3:
        return {"body": content, "log": []}

    frontmatter_text = parts[1]
    body_text = parts[2]

    # Parse YAML frontmatter (simple key: value, no full YAML dependency)
    meta = {}
    for line in frontmatter_text.strip().splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        key = key.strip()
        if key == "tags":
            inner = value.strip().strip("[]")
            meta[key] = [t.strip() for t in inner.split(",") if t.strip()] if inner else []
        elif key == "acknowledged":
            meta[key] = value.strip().lower() == "true"
        else:
            meta[key] = value.strip()

    # Split body into message and log entries
    log_entries = []
    body_lines = []
    in_log = False
    for line in body_text.splitlines():
        if line.strip() == "## Log":
            in_log = True
            continue
        if in_log:
            stripped = line.strip()
            if stripped.startswith("- "):
                log_entries.append(stripped[2:])
            elif stripped:
                log_entries.append(stripped)
        else:
            body_lines.append(line)

    meta["body"] = "\n".join(body_lines).strip()
    meta["log"] = log_entries
    meta["file_path"] = str(file_path)
    return meta


def find_event_by_id(short_id, search_dir=None):
    """Find an event file by its short ID. Returns Path or None."""
    search_dir = search_dir or EVENTS_DIR
    for path in Path(search_dir).rglob(f"*-{short_id}.event"):
        return path
    return None


# ── Enrich / Ack / Archive ──


def enrich_event(file_path, message, machine=None, agent=None):
    """Append a timestamped log entry to an event file. Uses flock for atomicity."""
    file_path = Path(file_path)
    machine = machine or MACHINE
    agent = agent or "unknown"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_line = f"- {ts} [{machine}/{agent}] {message}\n"

    with open(file_path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        content = f.read()
        if "## Log" in content:
            content = content.rstrip("\n") + "\n" + log_line
        else:
            content = content.rstrip("\n") + "\n\n## Log\n" + log_line
        f.seek(0)
        f.write(content)
        f.truncate()
        fcntl.flock(f, fcntl.LOCK_UN)


def ack_event(file_path):
    """Set acknowledged: true in event frontmatter."""
    file_path = Path(file_path)
    content = file_path.read_text()
    content = content.replace("acknowledged: false", "acknowledged: true")
    file_path.write_text(content)


def archive_event(file_path):
    """Move event from active dir to archive/YYYY-MM-DD/<topic>/. Returns new path."""
    file_path = Path(file_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    rel = file_path.parent.relative_to(EVENTS_DIR)
    archive_dir = ARCHIVE_DIR / today / rel
    archive_dir.mkdir(parents=True, exist_ok=True)

    dest = archive_dir / file_path.name
    file_path.rename(dest)
    return dest


def archive_topic(topic_pattern):
    """Archive all events matching a topic pattern. Returns list of archived paths."""
    topic_dir = EVENTS_DIR / topic_pattern.rstrip("/#")
    if not topic_dir.exists():
        return []
    archived = []
    for event_file in sorted(topic_dir.rglob("*.event")):
        archived.append(archive_event(event_file))
    return archived


def archive_stale(older_than_hours, acked_only=True):
    """Archive acked events older than N hours.

    Default policy: only archive events that are both acknowledged AND older
    than the cutoff. Set acked_only=False to archive any event older than the
    cutoff (including unacked), which is occasionally useful for purging
    abandoned tests.

    Returns a list of (short_id, reason, new_path) tuples for archived events.
    """
    if not EVENTS_DIR.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    archived = []
    for event_file in sorted(EVENTS_DIR.rglob("*.event")):
        try:
            meta = parse_event_file(event_file)
        except Exception:
            continue
        created_str = meta.get("created", "")
        if not created_str:
            continue
        try:
            created = datetime.strptime(created_str.rstrip("Z"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                created = datetime.strptime(created_str.rstrip("Z"), "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        if created >= cutoff:
            continue
        if acked_only and not meta.get("acknowledged", False):
            continue
        reason = "acked-and-stale" if meta.get("acknowledged") else "stale-unacked"
        try:
            new_path = archive_event(event_file)
            archived.append((meta.get("id", ""), reason, str(new_path)))
        except Exception:
            continue
    return archived


def purge_archive(older_than_days):
    """Delete archived events older than N days. Returns count of deleted files."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    deleted = 0
    if not ARCHIVE_DIR.exists():
        return 0
    for date_dir in sorted(ARCHIVE_DIR.iterdir()):
        if date_dir.is_dir() and date_dir.name <= cutoff_str:
            for f in date_dir.rglob("*.event"):
                f.unlink()
                deleted += 1
            for d in sorted(date_dir.rglob("*"), reverse=True):
                if d.is_dir():
                    try:
                        d.rmdir()
                    except OSError:
                        pass
            try:
                date_dir.rmdir()
            except OSError:
                pass
    return deleted


# ── DynamoDB Operations ──


def _get_table():
    """Get DynamoDB table resource. Import boto3 lazily to keep CLI fast."""
    import boto3
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    return dynamodb.Table(TABLE_NAME)


def make_dynamo_event_id(short_id, machine, agent):
    """Create a DynamoDB sort key for lexicographic ordering."""
    now = datetime.now(timezone.utc)
    micro = f"{now.microsecond:06d}"
    return f"{now.strftime('%Y-%m-%dT%H:%M:%S')}.{micro}Z#{machine}#{short_id}"


def dynamo_put_event(event_id, topic, tags, source_machine, source_agent, message):
    """Write an event to DynamoDB. Called in background — errors are silent."""
    try:
        table = _get_table()
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        sort_key = make_dynamo_event_id(event_id, source_machine, source_agent)

        item = {
            "date": today,
            "event_id": sort_key,
            "short_id": event_id,
            "topic": topic,
            "tags": tags,
            "source_machine": source_machine,
            "source_agent": source_agent,
            "body": message,
            "log_entries": [],
            "acknowledged": False,
            "created": created,
            "last_modified": created,
        }
        table.put_item(Item=item)
    except Exception as e:
        print(f"[eventbus] DynamoDB write failed (non-fatal): {e}", file=__import__("sys").stderr)


def dynamo_enrich_event(short_id, log_entry, days_back=7):
    """Append a log entry to an event in DynamoDB and bump last_modified."""
    try:
        from boto3.dynamodb.conditions import Key, Attr
        table = _get_table()
        now = datetime.now(timezone.utc)

        for i in range(days_back):
            day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            resp = table.query(
                KeyConditionExpression=Key("date").eq(day),
                FilterExpression=Attr("short_id").eq(short_id),
            )
            items = resp.get("Items", [])
            if items:
                item = items[0]
                table.update_item(
                    Key={"date": day, "event_id": item["event_id"]},
                    UpdateExpression="SET log_entries = list_append(log_entries, :entry), last_modified = :ts",
                    ExpressionAttributeValues={
                        ":entry": [log_entry],
                        ":ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                )
                return True
        return False
    except Exception as e:
        print(f"[eventbus] DynamoDB enrich failed (non-fatal): {e}", file=__import__("sys").stderr)
        return False


def dynamo_query_new(since_ts=None, exclude_machine=None, days_back=1):
    """Query DynamoDB for events, optionally filtering by timestamp and machine."""
    from boto3.dynamodb.conditions import Key
    table = _get_table()
    now = datetime.now(timezone.utc)
    events = []

    for i in range(days_back):
        day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        resp = table.query(
            KeyConditionExpression=Key("date").eq(day),
            ScanIndexForward=True,
        )
        events.extend(resp.get("Items", []))

    if since_ts:
        events = [e for e in events if e.get("created", "") > since_ts]
    if exclude_machine:
        events = [e for e in events if e.get("source_machine") != exclude_machine]

    return events


def dynamo_query_modified(since_ts, exclude_machine=None):
    """Query for events modified (enriched) since a given timestamp."""
    from boto3.dynamodb.conditions import Key, Attr
    table = _get_table()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    resp = table.query(
        KeyConditionExpression=Key("date").eq(today),
        FilterExpression=Attr("last_modified").gt(since_ts),
    )
    events = resp.get("Items", [])
    if exclude_machine:
        events = [e for e in events if e.get("source_machine") != exclude_machine]
    return events


# ── Query Helpers ──


def _parse_iso(ts_str):
    """Parse an ISO timestamp string (with or without microseconds) to a datetime.

    Handles both '%Y-%m-%dT%H:%M:%SZ' and '%Y-%m-%dT%H:%M:%S.%fZ' formats.
    Returns datetime.min on empty/invalid input.
    """
    if not ts_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    ts_str = ts_str.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.min.replace(tzinfo=timezone.utc)


def parse_duration(duration_str):
    """Parse a human duration string (e.g. '1h', '30m', '7d') into an ISO timestamp.

    Returns ISO timestamp for (now - duration). Returns the string unchanged if
    it's already an ISO timestamp.
    """
    if not duration_str:
        return None
    if "T" in duration_str:
        return duration_str  # Already an ISO timestamp

    units = {"m": "minutes", "h": "hours", "d": "days"}
    for suffix, kwarg in units.items():
        if duration_str.endswith(suffix):
            value = int(duration_str[:-1])
            cutoff = datetime.now(timezone.utc) - timedelta(**{kwarg: value})
            return cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    return duration_str  # Fallback: pass through


def list_events(topic_pattern, since=None):
    """List parsed events under a topic. Use '#' suffix for recursive.

    Args:
        topic_pattern: e.g. 'project/llm-server/task' (exact) or 'project/llm-server/#' (recursive)
        since: ISO timestamp or human duration ('1h', '30m', '7d').

    Returns: List of parsed event dicts, sorted by filename (chronological).
    """
    since = parse_duration(since)
    recursive = topic_pattern.endswith("/#") or topic_pattern.endswith("#")
    topic_dir = EVENTS_DIR / topic_pattern.rstrip("/#")

    if not topic_dir.exists():
        return []

    if recursive:
        files = sorted(topic_dir.rglob("*.event"))
    else:
        files = sorted(topic_dir.glob("*.event"))

    if since:
        # Add 1 second so a second-precision cutoff "T12:00:05Z" means
        # "events written in second :06 or later" (i.e., strictly after :05).
        since_epoch = _parse_iso(since).timestamp() + 1.0
        files = [f for f in files if f.stat().st_mtime > since_epoch]

    return [parse_event_file(f) for f in files]


def recent_events(count=10):
    """Return the most recent N events across all topics, sorted by filename."""
    all_files = sorted(EVENTS_DIR.rglob("*.event"))
    latest = all_files[-count:] if len(all_files) > count else all_files
    return [parse_event_file(f) for f in latest]


def list_topics():
    """List all topic directories that contain events, with counts.

    Returns: List of (topic_string, event_count) tuples.
    """
    topics = []
    if not EVENTS_DIR.exists():
        return topics
    for event_file in EVENTS_DIR.rglob("*.event"):
        topic = str(event_file.parent.relative_to(EVENTS_DIR))
        topics.append(topic)

    from collections import Counter
    counts = Counter(topics)
    return sorted(counts.items())

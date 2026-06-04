#!/usr/bin/env python3
"""Raspy watcher v5: mesh-capable adapter for Pi.

This daemon consumes JSON mesh events from ~/agent-events/events, participates in
claim/lease protocol, executes tasks through OpenClaw in raspy's Slack session,
and emits heartbeat/progress/result/fail events.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(os.environ.get("EVENTBUS_DIR", Path.home() / "agent-events"))
BUS_LIB = BASE_DIR / "lib"
sys.path.insert(0, str(BUS_LIB))

from bus_protocol import (  # noqa: E402
    emit_event,
    is_eligible_for,
    load_manifest,
    read_thread,
    recent_events,
)

MANIFEST_PATH = BASE_DIR / "manifests" / "raspy.json"
EVENTS_DIR = BASE_DIR / "events"
HEARTBEAT_FILE = BASE_DIR / ".raspy-watcher-heartbeat"
PID_FILE = BASE_DIR / ".raspy-watcher.pid"

OPENCLAW_BIN = os.environ.get("OPENCLAW_BIN", "/home/pmasior/.npm-global/bin/openclaw")
SLACK_CHANNEL = os.environ.get("RASPY_SLACK_CHANNEL", "C0ADEU0060H")
SLACK_CHANNEL_KEY = SLACK_CHANNEL.lower()
SESSIONS_FILE = Path.home() / ".openclaw/agents/main/sessions/sessions.json"
MAX_RETRIES = 5

_cached_session_id: str | None = None


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def source() -> dict:
    return {"agent": "raspy", "machine": "pi", "session_id": None}


def write_heartbeat_file() -> None:
    HEARTBEAT_FILE.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))


def write_pid() -> None:
    PID_FILE.write_text(str(os.getpid()))


def get_slack_session_id() -> str | None:
    """Resolve raspy Slack session id from openclaw session list."""
    global _cached_session_id
    if _cached_session_id:
        return _cached_session_id

    try:
        proc = subprocess.run(
            [OPENCLAW_BIN, "sessions", "--json"],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if proc.returncode == 0:
            data = json.loads(proc.stdout)
            sessions = data if isinstance(data, list) else data.get("sessions", [])
            for session in sessions:
                key = str(session.get("key", session.get("sessionKey", ""))).lower()
                if SLACK_CHANNEL_KEY in key and "thread" not in key:
                    sid = session.get("id", session.get("sessionId"))
                    if sid:
                        _cached_session_id = str(sid)
                        return _cached_session_id
    except Exception:
        pass

    try:
        data = json.loads(SESSIONS_FILE.read_text())
        for key, entry in data.items():
            if SLACK_CHANNEL_KEY in key.lower() and "thread" not in key.lower() and isinstance(entry, dict):
                sid = entry.get("id", entry.get("sessionId"))
                if sid:
                    _cached_session_id = str(sid)
                    return _cached_session_id
    except Exception:
        pass

    return None


def resolve_required_capabilities(event: dict) -> list[str]:
    if event.get("type") == "task.request":
        caps = event.get("payload", {}).get("capabilities", [])
        return caps if isinstance(caps, list) else []

    if event.get("type") != "task.reassign":
        return []

    payload = event.get("payload", {})
    original_task_id = payload.get("original_task_id")
    if original_task_id:
        for prior in read_thread(EVENTS_DIR, event.get("thread_id", "")):
            if prior.get("id") == original_task_id and prior.get("type") == "task.request":
                caps = prior.get("payload", {}).get("capabilities", [])
                return caps if isinstance(caps, list) else []

    fallback = payload.get("capabilities", [])
    return fallback if isinstance(fallback, list) else []


def task_id_for_claiming(event: dict) -> str:
    if event.get("type") == "task.request":
        return str(event.get("id", ""))
    return str(event.get("payload", {}).get("original_task_id", event.get("id", "")))


def evaluate_event(event: dict, manifest: dict) -> tuple[bool, str]:
    if event.get("type") not in {"task.request", "task.reassign"}:
        return False, "wrong type"

    payload = event.get("payload", {})
    retry_count = int(payload.get("retry_count", 0))
    if retry_count >= MAX_RETRIES:
        return False, "retry cap reached"

    required = resolve_required_capabilities(event)
    if not is_eligible_for(required, manifest):
        return False, "capability mismatch"

    if event.get("type") == "task.reassign":
        prior = payload.get("prior_claimers", [])
        if isinstance(prior, list) and manifest.get("agent") in prior:
            return False, "self in prior_claimers"

    return True, "eligible"


def has_existing_claim(task_id: str) -> bool:
    for claim in recent_events(EVENTS_DIR, filter_types={"task.claim"}):
        if claim.get("payload", {}).get("claiming_task_id") == task_id:
            return True
    return False


def try_claim(event: dict, manifest: dict) -> bool:
    task_id = task_id_for_claiming(event)
    if not task_id:
        return False

    jitter_s = int(manifest.get("jitter_ms", 0)) / 1000.0
    if jitter_s > 0:
        time.sleep(jitter_s)

    if has_existing_claim(task_id):
        return False

    emit_event(
        EVENTS_DIR,
        type="task.claim",
        thread_id=event.get("thread_id", ""),
        source=source(),
        payload={
            "claiming_task_id": task_id,
            "lease_duration_s": int(manifest.get("lease_duration_s_default", 90)),
            "expected_completion_s": int(manifest.get("lease_duration_s_default", 90)),
        },
    )

    time.sleep(0.5)
    claims = [
        claim
        for claim in recent_events(EVENTS_DIR, filter_types={"task.claim"})
        if claim.get("payload", {}).get("claiming_task_id") == task_id
    ]
    if not claims:
        return False

    winner = min(claims, key=lambda e: (e.get("created_at", ""), e.get("id", "")))
    return winner.get("source", {}).get("agent") == manifest.get("agent")


def run_openclaw(task_text: str) -> tuple[bool, str, str]:
    sid = get_slack_session_id()
    if not sid:
        return False, "", "cannot resolve slack session id"

    try:
        proc = subprocess.run(
            [
                OPENCLAW_BIN,
                "agent",
                "--session-id",
                sid,
                "--message",
                task_text,
                "--deliver",
                "--reply-channel",
                "slack",
                "--reply-to",
                SLACK_CHANNEL,
                "--timeout",
                "180",
            ],
            capture_output=True,
            text=True,
            timeout=240,
        )
    except subprocess.TimeoutExpired:
        return False, "", "openclaw timeout"
    except Exception as exc:
        return False, "", str(exc)

    return proc.returncode == 0, proc.stdout, proc.stderr


class Watcher:
    def __init__(self) -> None:
        self.manifest = load_manifest(MANIFEST_PATH.parent, "raspy")
        self._stop = threading.Event()
        self._seen: set[str] = set()
        self.active_claims: dict[str, threading.Thread] = {}

    def emit_heartbeat(self) -> None:
        emit_event(
            EVENTS_DIR,
            type="heartbeat",
            thread_id="raspy-heartbeats",
            source=source(),
            payload={
                "capabilities": self.manifest.get("capabilities", []),
                "adapter_version": "v5",
                "load": {
                    "concurrent_tasks": len(self.active_claims),
                    "max_concurrent": int(self.manifest.get("max_concurrent", 1)),
                },
            },
        )
        write_heartbeat_file()

    def heartbeat_loop(self) -> None:
        interval = int(self.manifest.get("heartbeat_interval_s", 60))
        while not self._stop.is_set():
            try:
                self.emit_heartbeat()
            except Exception:
                pass
            self._stop.wait(interval)

    def execute_claim(self, event: dict) -> None:
        task_id = task_id_for_claiming(event)
        thread_id = event.get("thread_id", "")
        task = event.get("payload", {}).get("task", "")

        progress_stop = threading.Event()

        def progress_loop() -> None:
            pct = 10
            while not progress_stop.is_set():
                emit_event(
                    EVENTS_DIR,
                    type="task.progress",
                    thread_id=thread_id,
                    source=source(),
                    payload={
                        "claiming_task_id": task_id,
                        "percent": pct,
                        "note": "raspy working",
                    },
                )
                progress_stop.wait(30)
                pct = min(pct + 10, 90)

        pt = threading.Thread(target=progress_loop, daemon=True)
        pt.start()

        started = time.time()
        ok, stdout, stderr = run_openclaw(task)
        wall_ms = int((time.time() - started) * 1000)
        progress_stop.set()

        if ok:
            emit_event(
                EVENTS_DIR,
                type="task.result",
                thread_id=thread_id,
                source=source(),
                payload={
                    "claiming_task_id": task_id,
                    "output": (stdout or "").strip()[:120000],
                    "artifacts": [],
                    "usage": {"input_tokens": 0, "output_tokens": 0, "wall_ms": wall_ms},
                },
            )
        else:
            emit_event(
                EVENTS_DIR,
                type="task.fail",
                thread_id=thread_id,
                source=source(),
                payload={
                    "claiming_task_id": task_id,
                    "reason": "raspy_error",
                    "detail": (stderr or "unknown error")[:12000],
                    "escalate_to_human": False,
                },
            )

        self.active_claims.pop(task_id, None)

    def serve_one(self, event: dict) -> None:
        eligible, reason = evaluate_event(event, self.manifest)
        if not eligible:
            return

        if len(self.active_claims) >= int(self.manifest.get("max_concurrent", 1)):
            return

        if not try_claim(event, self.manifest):
            return

        task_id = task_id_for_claiming(event)
        worker = threading.Thread(target=self.execute_claim, args=(event,), daemon=True)
        self.active_claims[task_id] = worker
        worker.start()

    def subscribe_loop(self) -> None:
        EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        for event in recent_events(EVENTS_DIR):
            eid = event.get("id")
            if eid:
                self._seen.add(eid)

        while not self._stop.is_set():
            try:
                for event in recent_events(EVENTS_DIR, filter_types={"task.request", "task.reassign"}):
                    eid = event.get("id")
                    if not eid or eid in self._seen:
                        continue
                    self._seen.add(eid)
                    self.serve_one(event)
            except Exception:
                pass
            self._stop.wait(2.0)

    def shutdown(self) -> None:
        self._stop.set()
        for claim_id in list(self.active_claims.keys()):
            try:
                emit_event(
                    EVENTS_DIR,
                    type="task.fail",
                    thread_id="raspy-shutdown",
                    source=source(),
                    payload={
                        "claiming_task_id": claim_id,
                        "reason": "adapter_shutdown",
                        "detail": "raspy watcher stopping",
                        "escalate_to_human": False,
                    },
                )
            except Exception:
                pass

    def run(self) -> None:
        hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb.start()
        self.subscribe_loop()


def main() -> None:
    parser = argparse.ArgumentParser(description="raspy watcher v5")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    watcher = Watcher()
    write_pid()

    def _handle_signal(*_args):
        watcher.shutdown()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if args.once:
        for event in recent_events(EVENTS_DIR, filter_types={"task.request", "task.reassign"}):
            watcher.serve_one(event)
        for t in list(watcher.active_claims.values()):
            t.join()
        return

    watcher.run()


if __name__ == "__main__":
    main()

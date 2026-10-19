"""Copied assistant log entries must not increase token usage or cost."""

import json
from copy import deepcopy

import pytest

from sniffly.core.processor import ClaudeLogProcessor
from sniffly.utils import pricing


@pytest.fixture(autouse=True)
def fixed_pricing(monkeypatch):
    """Use synthetic rates without reading caches or fetching remote pricing."""
    monkeypatch.setattr(
        pricing,
        "get_dynamic_pricing",
        lambda: {
            "test-model": {
                "input_cost_per_token": 1,
                "output_cost_per_token": 1,
                "cache_creation_cost_per_token": 1,
                "cache_read_cost_per_token": 1,
            }
        },
    )


def conversation():
    user = {
        "type": "user",
        "uuid": "user-entry",
        "timestamp": "2025-08-01T10:00:00Z",
        "message": {"role": "user", "content": "Synthetic test request"},
    }
    assistant = {
        "type": "assistant",
        "uuid": "assistant-entry",
        "parentUuid": "user-entry",
        "timestamp": "2025-08-01T10:00:01Z",
        "message": {
            "id": "synthetic-message",
            "role": "assistant",
            "model": "test-model",
            "content": [{"type": "text", "text": "Synthetic response"}],
            "usage": {
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_creation_input_tokens": 30,
                "cache_read_input_tokens": 40,
            },
        },
    }
    return user, assistant


def write_log(path, entries):
    path.write_text("".join(json.dumps(entry) + "\n" for entry in entries), encoding="utf-8")


@pytest.mark.parametrize("copies", [1, 2, 3])
@pytest.mark.parametrize("across_files", [False, True])
def test_copied_assistant_usage_counted_once(tmp_path, copies, across_files):
    user, assistant = conversation()
    if across_files:
        for index in range(copies):
            write_log(tmp_path / f"session-{index}.jsonl", [user, assistant])
    else:
        write_log(tmp_path / "session.jsonl", [user] + [assistant] * copies)

    processor = ClaudeLogProcessor(str(tmp_path))
    # A second refresh must rebuild deduplication state instead of losing all usage.
    for _ in range(2):
        messages, stats = processor.process_logs()
        assistants = [msg for msg in messages if msg["type"] == "assistant"]
        assert len(assistants) == 1
        expected = {"input": 100, "output": 20, "cache_creation": 30, "cache_read": 40}
        assert assistants[0]["tokens"] == expected
        assert stats["overview"]["total_tokens"] == expected
        assert stats["overview"]["total_cost"] == 190


@pytest.mark.parametrize("second_uuid", ["another-entry", None, ""])
def test_distinct_stream_entries_keep_their_usage(tmp_path, second_uuid):
    user, first = conversation()
    second = deepcopy(first)
    if second_uuid is None:
        first.pop("uuid")
        second.pop("uuid")
    else:
        second["uuid"] = second_uuid
        if not second_uuid:
            first["uuid"] = ""
    second["timestamp"] = "2025-08-01T10:00:02Z"
    second["message"]["content"] = [{"type": "text", "text": "Another response block"}]
    write_log(tmp_path / "session.jsonl", [user, first, second])

    messages, stats = ClaudeLogProcessor(str(tmp_path)).process_logs()
    assistant = next(msg for msg in messages if msg["type"] == "assistant")
    expected = {"input": 200, "output": 40, "cache_creation": 60, "cache_read": 80}
    assert assistant["tokens"] == expected
    assert "Another response block" in assistant["content"]
    assert stats["overview"]["total_tokens"] == expected
    assert stats["overview"]["total_cost"] == 380

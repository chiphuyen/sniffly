"""Regression coverage for configured overview history (issue #23)."""

import json
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from sniffly.core import global_aggregator


@pytest.fixture
def history(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 6, 1)

    monkeypatch.setattr(global_aggregator, "datetime", FixedDateTime)
    today = FixedDateTime.now().date()
    daily = {}
    for age in range(-1, 401):
        daily[(today - timedelta(days=age)).isoformat()] = {
            "tokens": {"input": 10, "output": 20},
            "cost": {
                "total": 3,
                "by_model": {"synthetic": {"input_cost": 1, "output_cost": 1, "cache_read_cost": 1}},
            },
        }
    stats = {"overview": {"total_cost": 9999, "total_tokens": {"input": 12345}}, "daily_stats": daily}
    memory = Mock()
    memory.get.return_value = ([], stats)
    files = Mock()
    files.get_cached_stats.return_value = stats
    projects = [{"dir_name": "synthetic", "log_path": "synthetic", "in_cache": True}]
    return today, memory, files, projects


@pytest.mark.asyncio
@pytest.mark.parametrize("days", [1, 7, 30, 90, 365])
async def test_history_window_and_totals(history, days):
    today, memory, files, projects = history
    aggregator = global_aggregator.GlobalStatsAggregator(memory, files)
    result = await aggregator.get_global_stats(projects, days=days)
    # Keep at least 30 days for the overview's fixed 30-day summary cards.
    expected = max(30, days)
    for key in ("daily_token_usage", "daily_costs"):
        rows = result[key]
        assert len(rows) == expected
        assert rows[0]["date"] == (today - timedelta(days=expected - 1)).isoformat()
        assert rows[-1]["date"] == today.isoformat()
    assert sum(row["input"] for row in result["daily_token_usage"]) == expected * 10
    assert sum(row["cost"] for row in result["daily_costs"]) == expected * 3
    assert result["daily_costs"][0]["cache_cost"] == 1
    assert result["total_cost"] == 9999
    assert result["total_input_tokens"] == 12345


@pytest.mark.asyncio
@pytest.mark.parametrize("days", [7, 30, 90])
async def test_endpoint_uses_configured_history(history, monkeypatch, days):
    from sniffly import server
    from sniffly.utils import log_finder

    _, memory, files, projects = history
    monkeypatch.setattr(server, "memory_cache", memory)
    monkeypatch.setattr(server, "cache_service", files)
    monkeypatch.setattr(server.config, "get", lambda key: days)
    monkeypatch.setattr(log_finder, "get_all_projects_with_metadata", lambda: projects)
    response = await server.get_global_stats()
    assert response.status_code == 200
    result = json.loads(response.body)
    assert result["config"]["max_date_range_days"] == days
    assert len(result["daily_token_usage"]) == max(30, days)
    assert len(result["daily_costs"]) == max(30, days)


@pytest.mark.asyncio
async def test_empty_history_is_zero_filled(history):
    _, memory, files, _ = history
    result = await global_aggregator.GlobalStatsAggregator(memory, files).get_global_stats([], days=90)
    assert len(result["daily_token_usage"]) == 90
    assert all(row["cost"] == 0 for row in result["daily_costs"])
    assert all(row["input"] == row["output"] == 0 for row in result["daily_token_usage"])

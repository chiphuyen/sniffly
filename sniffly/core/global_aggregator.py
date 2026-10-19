"""
Global statistics aggregator for Claude Analytics.
Aggregates statistics across all projects for the overview page.
"""

import asyncio
import logging
from datetime import datetime, timedelta

# Set up logging
logger = logging.getLogger(__name__)


class GlobalStatsAggregator:
    """Aggregates statistics across all Claude projects."""

    def __init__(self, memory_cache, file_cache):
        """
        Initialize the aggregator.

        Args:
            memory_cache: Memory cache instance
            file_cache: File cache service instance
        """
        self.memory_cache = memory_cache
        self.file_cache = file_cache

    async def get_global_stats(self, projects: list[dict], days: int = 30) -> dict:
        """
        Aggregate statistics across all projects.

        Args:
            projects: List of project dictionaries from get_all_projects_with_metadata()
            days: Chart history length; at least 30 days are retained for summary cards

        Returns:
            Dictionary containing aggregated global statistics
        """
        logger.debug(f"Starting global stats aggregation for {len(projects)} projects")

        # Initialize aggregated data
        # IMPORTANT: We track both all-time totals and daily totals separately:
        # - All-time totals come from project overview stats (includes ALL messages)
        # - Daily totals come from daily_stats (only includes messages with timestamps)
        # Totals may differ if some messages lack timestamps or fall outside the chart window.

        daily_tokens = {}  # date -> {input: 0, output: 0, cache_read: 0}
        daily_costs = {}  # date -> cost within the chart window
        daily_cost_breakdown = {}  # date -> {input: 0, output: 0, cache: 0}

        # All-time totals (from project overviews)
        total_input = 0
        total_output = 0
        total_cache_read = 0
        total_cache_write = 0
        total_commands = 0
        total_cost_all_time = 0.0  # Actual all-time cost from overview.total_cost

        earliest_timestamp = None
        latest_timestamp = None

        # Keep enough history for both configurable charts and fixed 30-day summaries.
        history_days = max(30, days)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=history_days - 1)

        # Initialize daily data with zeros
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            daily_tokens[date_str] = {"input": 0, "output": 0}
            daily_costs[date_str] = 0.0
            daily_cost_breakdown[date_str] = {"input": 0.0, "output": 0.0, "cache": 0.0}
            current_date += timedelta(days=1)

        # Process each project
        for project in projects:
            project_name = project.get("dir_name", "unknown")
            stats = await self._get_project_stats(project)

            if stats:
                # Aggregate all-time totals from overview section
                # These include ALL messages, even those without timestamps
                overview = stats.get("overview", {})
                total_tokens = overview.get("total_tokens", {})
                total_input += total_tokens.get("input", 0)
                total_output += total_tokens.get("output", 0)
                total_cache_read += total_tokens.get("cache_read", 0)
                total_cache_write += total_tokens.get("cache_creation", 0)

                # Get user commands from user_interactions
                user_interactions = stats.get("user_interactions", {})
                total_commands += user_interactions.get("user_commands_analyzed", 0)

                # Get actual all-time cost from overview (not sum of daily_stats)
                # This ensures all-time cost includes messages without timestamps
                total_cost_all_time += overview.get("total_cost", 0)

                # Aggregate daily stats within the chart window
                if "daily_stats" in stats:
                    if not isinstance(stats["daily_stats"], dict):
                        logger.warning(
                            f"Project {project_name}: daily_stats is not a dict, got {type(stats['daily_stats']).__name__}"
                        )
                    else:
                        # Handle dictionary format (the actual format from stats.py)
                        for date_str, day_data in stats["daily_stats"].items():
                            try:
                                date_obj = datetime.fromisoformat(date_str).date()
                                if start_date <= date_obj <= end_date:
                                    if date_str in daily_tokens:
                                        # Extract tokens from the nested structure
                                        tokens = day_data.get("tokens", {})
                                        daily_tokens[date_str]["input"] += tokens.get("input", 0)
                                        daily_tokens[date_str]["output"] += tokens.get("output", 0)
                                        # Extract cost from the nested structure
                                        cost_data = day_data.get("cost", {})
                                        daily_costs[date_str] += cost_data.get("total", 0)

                                        # Extract cost breakdown from by_model
                                        by_model = cost_data.get("by_model", {})
                                        for _, model_costs in by_model.items():
                                            daily_cost_breakdown[date_str]["input"] += model_costs.get("input_cost", 0)
                                            daily_cost_breakdown[date_str]["output"] += model_costs.get(
                                                "output_cost", 0
                                            )
                                            daily_cost_breakdown[date_str]["cache"] += model_costs.get(
                                                "cache_creation_cost", 0
                                            ) + model_costs.get("cache_read_cost", 0)
                            except (ValueError, KeyError, TypeError, AttributeError) as e:
                                logger.error(
                                    f"Project {project_name}: Error processing daily_stats for date {date_str}: "
                                    f"{type(e).__name__}: {e}"
                                )
                                continue
                else:
                    logger.info(f"Project {project_name}: No daily_stats found in statistics")

                # Track earliest and latest usage
                if "first_message_date" in stats and stats["first_message_date"]:
                    try:
                        first_date = datetime.fromisoformat(stats["first_message_date"].replace("Z", "+00:00"))
                        if not earliest_timestamp or first_date < earliest_timestamp:
                            earliest_timestamp = first_date
                    except (ValueError, TypeError) as e:
                        logger.error(
                            f"Project {project_name}: Error parsing first_message_date "
                            f"'{stats['first_message_date']}': {type(e).__name__}: {e}"
                        )

                if "last_message_date" in stats and stats["last_message_date"]:
                    try:
                        last_date = datetime.fromisoformat(stats["last_message_date"].replace("Z", "+00:00"))
                        if not latest_timestamp or last_date > latest_timestamp:
                            latest_timestamp = last_date
                    except (ValueError, TypeError) as e:
                        logger.error(
                            f"Project {project_name}: Error parsing last_message_date "
                            f"'{stats['last_message_date']}': {type(e).__name__}: {e}"
                        )
            else:
                logger.debug(f"Project {project_name}: No stats available")

        # Calculate chart-window total cost for logging
        total_cost_window = sum(daily_costs.values())

        # Convert daily data to list format for charts
        daily_token_list = []
        daily_cost_list = []

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            daily_token_list.append(
                {"date": date_str, "input": daily_tokens[date_str]["input"], "output": daily_tokens[date_str]["output"]}
            )
            daily_cost_list.append(
                {
                    "date": date_str,
                    "cost": daily_costs[date_str],
                    "input_cost": daily_cost_breakdown[date_str]["input"],
                    "output_cost": daily_cost_breakdown[date_str]["output"],
                    "cache_cost": daily_cost_breakdown[date_str]["cache"],
                }
            )
            current_date += timedelta(days=1)

        # Log aggregation summary
        logger.debug(
            f"Global stats aggregation complete: {len(projects)} projects, "
            f"{total_commands} commands, {total_input + total_output} total tokens, "
            f"${total_cost_all_time:.2f} all-time cost, ${total_cost_window:.2f} {history_days}-day cost"
        )

        return {
            "total_projects": len(projects),
            "first_use_date": earliest_timestamp.isoformat() if earliest_timestamp else None,
            "last_use_date": latest_timestamp.isoformat() if latest_timestamp else None,
            # All-time totals (includes messages without timestamps)
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_cache_read_tokens": total_cache_read,
            "total_cache_write_tokens": total_cache_write,
            "total_commands": total_commands,
            "total_cost": total_cost_all_time,  # From overview.total_cost, not sum of daily
            # Chart-window data (only includes messages with timestamps)
            "daily_token_usage": daily_token_list,
            "daily_costs": daily_cost_list,
        }

    async def _get_project_stats(self, project: dict) -> dict | None:
        """
        Get statistics for a single project.

        Args:
            project: Project dictionary with log_path

        Returns:
            Statistics dictionary or None if not available
        """
        log_path = project["log_path"]

        # Try memory cache first
        if project.get("in_cache"):
            cache_result = self.memory_cache.get(log_path)
            if cache_result:
                _, stats = cache_result
                return stats

        # Try file cache
        stats = self.file_cache.get_cached_stats(log_path)
        if stats:
            return stats

        # Stats not available - would need to process
        # For now, return None to indicate unavailable
        # In production, could queue for background processing
        return None

    async def process_uncached_projects(self, projects: list[dict], limit: int = 5) -> int:
        """
        Process uncached projects in the background.

        Args:
            projects: List of project dictionaries
            limit: Maximum number of projects to process

        Returns:
            Number of projects processed
        """
        from sniffly.core.processor import ClaudeLogProcessor

        processed = 0
        uncached_projects = [p for p in projects if not p.get("in_cache") and not p.get("stats")]

        for project in uncached_projects[:limit]:
            try:
                log_path = project["log_path"]
                processor = ClaudeLogProcessor(log_path)
                messages, stats = processor.process_logs()

                # Save to caches
                self.file_cache.save_cached_stats(log_path, stats)
                self.file_cache.save_cached_messages(log_path, messages)
                self.memory_cache.put(log_path, messages, stats)

                processed += 1

                # Yield to other tasks
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error processing uncached project {project['dir_name']}: {type(e).__name__}: {e}")

        return processed

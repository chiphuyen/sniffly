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

    async def get_global_stats(self, projects: list[dict]) -> dict:
        """
        Aggregate statistics across all projects.

        Args:
            projects: List of project dictionaries from get_all_projects_with_metadata()

        Returns:
            Dictionary containing aggregated global statistics
        """
        logger.info(f"Starting global stats aggregation for {len(projects)} projects")

        # Initialize aggregated data
        # IMPORTANT: We track both all-time totals and 30-day totals separately:
        # - All-time totals come from project overview stats (includes ALL messages)
        # - 30-day totals come from summing daily_stats (only includes messages with timestamps)
        # This may cause slight discrepancies for users with <30 days of usage if some messages lack timestamps

        daily_tokens = {}  # date -> {input: 0, output: 0, cache_read: 0}
        daily_costs = {}  # date -> cost (last 30 days only)
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

        # Get the last 30 days for chart data
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)

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

                # Aggregate daily stats for last 30 days
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

        # Calculate 30-day total cost for logging
        total_cost_30_days = sum(daily_costs.values())

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
        logger.info(
            f"Global stats aggregation complete: {len(projects)} projects, "
            f"{total_commands} commands, {total_input + total_output} total tokens, "
            f"${total_cost_all_time:.2f} all-time cost, ${total_cost_30_days:.2f} 30-day cost"
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
            # 30-day data (only includes messages with timestamps)
            "daily_token_usage": daily_token_list,  # Last 30 days
            "daily_costs": daily_cost_list,  # Last 30 days with breakdown
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

    async def get_rollup_stats(self, rollup_name: str, child_projects: list[dict]) -> dict:
        """
        Aggregate statistics for a rollup from its child projects.
        
        Args:
            rollup_name: Name of the rollup
            child_projects: List of child project dictionaries
            
        Returns:
            Dictionary containing aggregated rollup statistics
        """
        logger.info(f"Aggregating rollup stats for '{rollup_name}' with {len(child_projects)} projects")
        
        # Initialize aggregated data
        total_input = 0
        total_output = 0
        total_cache_read = 0
        total_cache_write = 0
        total_commands = 0
        total_cost = 0.0
        
        earliest_timestamp = None
        latest_timestamp = None
        
        # Daily aggregation for last 30 days
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
        
        daily_tokens = {}
        daily_costs = {}
        daily_cost_breakdown = {}
        
        # Initialize daily data
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            daily_tokens[date_str] = {"input": 0, "output": 0}
            daily_costs[date_str] = 0.0
            daily_cost_breakdown[date_str] = {"input": 0.0, "output": 0.0, "cache": 0.0}
            current_date += timedelta(days=1)
        
        # Initialize aggregation containers for missing statistics
        total_messages = 0
        total_sessions = 0
        message_types = {"compact_summary": 0, "user": 0, "assistant": 0}
        
        # User interactions aggregation
        total_steps = 0
        tool_count_distributions = {}
        model_distributions = {}
        interruption_stats = {"commands": 0, "interruptions": 0}
        total_tools_used = 0
        commands_with_tools = 0
        search_tools_used = 0
        
        # Tools aggregation
        tools_usage = {}
        tools_errors = {}
        
        # Errors aggregation
        total_errors = 0
        error_categories = {}
        
        # Models aggregation
        models_stats = {}
        
        # Cache aggregation
        cache_stats = {"created": 0, "read": 0, "messages_with_read": 0, "messages_with_created": 0, "assistant_messages": 0}
        
        # Sessions aggregation
        sessions_stats = {"durations": [], "error_counts": [], "message_counts": []}

        # Process each child project
        for project in child_projects:
            stats = await self._get_project_stats(project)
            
            if stats:
                # Aggregate overview stats
                overview = stats.get("overview", {})
                total_tokens = overview.get("total_tokens", {})
                total_input += total_tokens.get("input", 0)
                total_output += total_tokens.get("output", 0)
                total_cache_read += total_tokens.get("cache_read", 0)
                total_cache_write += total_tokens.get("cache_creation", 0)
                
                # Aggregate overview metadata
                total_messages += overview.get("total_messages", 0)
                total_sessions += overview.get("sessions", 0)
                msg_types = overview.get("message_types", {})
                for msg_type, count in msg_types.items():
                    if msg_type in message_types:
                        message_types[msg_type] += count
                
                user_interactions = stats.get("user_interactions", {})
                total_commands += user_interactions.get("user_commands_analyzed", 0)
                
                # Aggregate user interaction stats
                total_steps += user_interactions.get("avg_steps_per_command", 0) * user_interactions.get("user_commands_analyzed", 0)
                
                # Aggregate tool count distribution
                tool_dist = user_interactions.get("tool_count_distribution", {})
                for count, freq in tool_dist.items():
                    tool_count_distributions[count] = tool_count_distributions.get(count, 0) + freq
                
                # Aggregate model distribution
                model_dist = user_interactions.get("model_distribution", {})
                for model, freq in model_dist.items():
                    model_distributions[model] = model_distributions.get(model, 0) + freq
                
                # Aggregate interruption stats
                interruption_stats["commands"] += user_interactions.get("user_commands_analyzed", 0)
                interruption_stats["interruptions"] += user_interactions.get("commands_followed_by_interruption", 0)
                
                # Aggregate tool usage stats
                tools_used = user_interactions.get("total_tools_used", 0)
                total_tools_used += tools_used
                commands_with_tools += user_interactions.get("commands_requiring_tools", 0)
                search_tools_used += user_interactions.get("total_search_tools", 0)
                
                # Aggregate tools section
                tools_section = stats.get("tools", {})
                usage_counts = tools_section.get("usage_counts", {})
                for tool, count in usage_counts.items():
                    tools_usage[tool] = tools_usage.get(tool, 0) + count
                
                error_counts = tools_section.get("error_counts", {})
                for tool, count in error_counts.items():
                    tools_errors[tool] = tools_errors.get(tool, 0) + count
                
                # Aggregate errors section
                errors_section = stats.get("errors", {})
                total_errors += errors_section.get("total", 0)
                error_cats = errors_section.get("by_category", {})
                for category, count in error_cats.items():
                    error_categories[category] = error_categories.get(category, 0) + count
                
                # Aggregate models section
                models_section = stats.get("models", {})
                for model, model_stats in models_section.items():
                    if model not in models_stats:
                        models_stats[model] = {"count": 0, "input_tokens": 0, "output_tokens": 0, 
                                             "cache_creation_tokens": 0, "cache_read_tokens": 0, "cost": 0}
                    models_stats[model]["count"] += model_stats.get("count", 0)
                    models_stats[model]["input_tokens"] += model_stats.get("input_tokens", 0)
                    models_stats[model]["output_tokens"] += model_stats.get("output_tokens", 0)
                    models_stats[model]["cache_creation_tokens"] += model_stats.get("cache_creation_tokens", 0)
                    models_stats[model]["cache_read_tokens"] += model_stats.get("cache_read_tokens", 0)
                    models_stats[model]["cost"] += model_stats.get("cost", 0)
                
                # Aggregate cache section
                cache_section = stats.get("cache", {})
                cache_stats["created"] += cache_section.get("total_created", 0)
                cache_stats["read"] += cache_section.get("total_read", 0)
                cache_stats["messages_with_read"] += cache_section.get("messages_with_cache_read", 0)
                cache_stats["messages_with_created"] += cache_section.get("messages_with_cache_created", 0)
                cache_stats["assistant_messages"] += cache_section.get("assistant_messages", 0)
                
                # Aggregate sessions section
                sessions_section = stats.get("sessions", {})
                sessions_stats["durations"].append(sessions_section.get("average_duration_seconds", 0))
                sessions_stats["error_counts"].append(sessions_section.get("sessions_with_errors", 0))
                sessions_stats["message_counts"].append(sessions_section.get("average_messages", 0))
                
                total_cost += overview.get("total_cost", 0)
                
                # Aggregate daily stats
                if "daily_stats" in stats and isinstance(stats["daily_stats"], dict):
                    for date_str, day_data in stats["daily_stats"].items():
                        try:
                            date_obj = datetime.fromisoformat(date_str).date()
                            if start_date <= date_obj <= end_date and date_str in daily_tokens:
                                tokens = day_data.get("tokens", {})
                                daily_tokens[date_str]["input"] += tokens.get("input", 0)
                                daily_tokens[date_str]["output"] += tokens.get("output", 0)
                                
                                cost_data = day_data.get("cost", {})
                                daily_costs[date_str] += cost_data.get("total", 0)
                                
                                # Aggregate cost breakdown
                                by_model = cost_data.get("by_model", {})
                                for _, model_costs in by_model.items():
                                    daily_cost_breakdown[date_str]["input"] += model_costs.get("input_cost", 0)
                                    daily_cost_breakdown[date_str]["output"] += model_costs.get("output_cost", 0)
                                    daily_cost_breakdown[date_str]["cache"] += (
                                        model_costs.get("cache_creation_cost", 0) + 
                                        model_costs.get("cache_read_cost", 0)
                                    )
                        except (ValueError, KeyError, TypeError, AttributeError):
                            continue
                
                # Track date range
                if "first_message_date" in stats and stats["first_message_date"]:
                    try:
                        first_date = datetime.fromisoformat(stats["first_message_date"].replace("Z", "+00:00"))
                        if not earliest_timestamp or first_date < earliest_timestamp:
                            earliest_timestamp = first_date
                    except (ValueError, TypeError):
                        pass
                
                if "last_message_date" in stats and stats["last_message_date"]:
                    try:
                        last_date = datetime.fromisoformat(stats["last_message_date"].replace("Z", "+00:00"))
                        if not latest_timestamp or last_date > latest_timestamp:
                            latest_timestamp = last_date
                    except (ValueError, TypeError):
                        pass
        
        # Convert daily data to list format
        daily_token_list = []
        daily_cost_list = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            daily_token_list.append({
                "date": date_str,
                "input": daily_tokens[date_str]["input"],
                "output": daily_tokens[date_str]["output"]
            })
            daily_cost_list.append({
                "date": date_str,
                "cost": daily_costs[date_str],
                "input_cost": daily_cost_breakdown[date_str]["input"],
                "output_cost": daily_cost_breakdown[date_str]["output"],
                "cache_cost": daily_cost_breakdown[date_str]["cache"]
            })
            current_date += timedelta(days=1)
        
        # Calculate aggregated metrics
        avg_steps_per_command = total_steps / total_commands if total_commands > 0 else 0
        interruption_rate = (interruption_stats["interruptions"] / interruption_stats["commands"] * 100) if interruption_stats["commands"] > 0 else 0
        
        # Calculate tool error rates
        tools_error_rates = {}
        for tool in tools_usage:
            error_count = tools_errors.get(tool, 0)
            usage_count = tools_usage[tool]
            tools_error_rates[tool] = error_count / usage_count if usage_count > 0 else 0
        
        # Calculate cache metrics
        cache_hit_rate = (cache_stats["messages_with_read"] / cache_stats["assistant_messages"] * 100) if cache_stats["assistant_messages"] > 0 else 0
        cache_efficiency = (cache_stats["read"] / cache_stats["created"] * 100) if cache_stats["created"] > 0 else 0
        tokens_saved = cache_stats["read"] - cache_stats["created"]
        cache_cost_saved = (cache_stats["read"] * 1.00) - (cache_stats["read"] * 0.10) - (cache_stats["created"] * 1.25)
        
        # Calculate session averages
        avg_session_duration = sum(sessions_stats["durations"]) / len(sessions_stats["durations"]) if sessions_stats["durations"] else 0
        avg_session_messages = sum(sessions_stats["message_counts"]) / len(sessions_stats["message_counts"]) if sessions_stats["message_counts"] else 0
        total_error_sessions = sum(sessions_stats["error_counts"])
        
        # Calculate overall error rate
        error_rate = (total_errors / total_messages) if total_messages > 0 else 0

        # Build rollup stats in same format as project stats
        rollup_stats = {
            "overview": {
                "project_name": rollup_name,
                "project_path": f"rollup:{rollup_name}",
                "log_dir_name": rollup_name,
                "total_tokens": {
                    "input": total_input,
                    "output": total_output,
                    "cache_read": total_cache_read,
                    "cache_creation": total_cache_write
                },
                "total_cost": total_cost,
                "date_range": {
                    "start": earliest_timestamp.isoformat() if earliest_timestamp else None,
                    "end": latest_timestamp.isoformat() if latest_timestamp else None
                },
                "message_types": message_types,
                "total_messages": total_messages,
                "sessions": total_sessions
            },
            "user_interactions": {
                "user_commands_analyzed": total_commands,
                "avg_tokens_per_command": round((total_input + total_output) / total_commands, 1) if total_commands > 0 else 0,
                "avg_steps_per_command": round(avg_steps_per_command, 2),
                "tool_count_distribution": tool_count_distributions,
                "model_distribution": model_distributions,
                "interruption_rate": round(interruption_rate, 1),
                "commands_followed_by_interruption": interruption_stats["interruptions"],
                "non_interruption_commands": interruption_stats["commands"] - interruption_stats["interruptions"],
                "percentage_requiring_tools": round((commands_with_tools / total_commands * 100), 1) if total_commands > 0 else 0,
                "commands_requiring_tools": commands_with_tools,
                "total_tools_used": total_tools_used,
                "total_search_tools": search_tools_used,
                "search_tool_percentage": round((search_tools_used / total_tools_used * 100), 1) if total_tools_used > 0 else 0
            },
            "tools": {
                "usage_counts": tools_usage,
                "error_counts": tools_errors,
                "error_rates": tools_error_rates
            },
            "errors": {
                "total": total_errors,
                "rate": round(error_rate, 1),
                "by_category": error_categories,
                "assistant_details": []  # No individual messages for rollups
            },
            "models": models_stats,
            "cache": {
                "total_created": cache_stats["created"],
                "total_read": cache_stats["read"],
                "messages_with_cache_read": cache_stats["messages_with_read"],
                "messages_with_cache_created": cache_stats["messages_with_created"],
                "assistant_messages": cache_stats["assistant_messages"],
                "hit_rate": round(cache_hit_rate, 1),
                "efficiency": round(min(100, cache_efficiency), 1),
                "tokens_saved": tokens_saved,
                "cost_saved_base_units": round(cache_cost_saved, 2),
                "break_even_achieved": cache_stats["read"] > cache_stats["created"],
                "cache_roi": round(((cache_stats["read"] / cache_stats["created"] - 1) * 100), 1) if cache_stats["created"] > 0 else 0
            },
            "sessions": {
                "count": total_sessions,
                "average_duration_seconds": avg_session_duration,
                "average_messages": avg_session_messages,
                "sessions_with_errors": total_error_sessions
            },
            "hourly_pattern": {
                "messages": {str(hour): 0 for hour in range(24)},
                "tokens": {str(hour): {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0} for hour in range(24)}
            },
            "daily_stats": {
                date_str: {
                    "tokens": {
                        "input": data["input"], 
                        "output": data["output"],
                        "cache_read": 0,  # Would need more complex daily aggregation
                        "cache_creation": 0
                    },
                    "cost": {
                        "total": daily_costs[date_str],
                        "by_model": {}  # Simplified for rollups
                    },
                    "sessions": 0,  # Would need session-level daily tracking
                    "errors": 0  # Would need error-level daily tracking
                }
                for date_str, data in zip(
                    [d.isoformat() for d in (start_date + timedelta(days=i) for i in range(30))],
                    daily_token_list
                )
            },
            "first_message_date": earliest_timestamp.isoformat() if earliest_timestamp else None,
            "last_message_date": latest_timestamp.isoformat() if latest_timestamp else None,
            "is_rollup": True,
            "rollup_name": rollup_name,
            "child_project_count": len(child_projects)
        }
        
        logger.info(
            f"Rollup '{rollup_name}' aggregation complete: {len(child_projects)} projects, "
            f"{total_commands} commands, {total_input + total_output} total tokens, "
            f"${total_cost:.2f} total cost"
        )
        
        return rollup_stats

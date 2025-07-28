#!/usr/bin/env python3
"""
Statistics generation module for Claude Analytics.

This module generates comprehensive statistics from processed Claude messages,
including:
- Overview statistics (tokens, costs, message counts)
- Tool usage analytics with error rates
- Session analytics and duration calculations
- Daily and hourly patterns with timezone support
- User interaction analysis with interruption rates
- Cache efficiency metrics
- Model usage breakdown

All date/time grouping supports timezone conversion for accurate local display.
"""

import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from ..utils.pricing import calculate_cost
from .constants import ERROR_PATTERNS, USER_INTERRUPTION_API_ERROR, USER_INTERRUPTION_PATTERNS


class StatisticsGenerator:
    """Generates comprehensive statistics from processed Claude messages.

    The generator uses pre-computed running statistics from the processor
    for efficiency, but recalculates time-based statistics with timezone
    support to ensure accurate local time display.
    """

    def __init__(self, log_directory: str, running_stats: dict[str, Any]):
        """
        Initialize the statistics generator.

        Args:
            log_directory: Path to the log directory (for project name extraction)
            running_stats: Pre-computed running statistics from processor
                          Note: daily_tokens in running_stats are UTC-based and
                          will be recalculated with timezone offset
        """
        self.log_directory = log_directory
        self.running_stats = running_stats
        # Cache for bash command search detection - trades memory for speed
        self._bash_search_cache = {}

    # Pre-compile regex patterns for better performance
    SEARCH_TOOL_NAMES = frozenset(["Grep", "LS", "Glob"])
    BASH_SEARCH_COMMANDS = frozenset(["ls", "grep", "rg", "find", "locate", "which", "whereis", "fd"])
    COMMAND_SPLIT_PATTERN = re.compile(r"[|;&]")

    def _is_search_tool(self, tool_name: str, tool_input: dict = None) -> bool:
        """Check if a tool is a search-related tool.

        Search tools include:
        - Direct search tools: Grep, LS, Glob
        - Bash commands with search commands: ls, grep, rg, find
        """
        # Direct search tools - use set lookup for O(1) performance
        if tool_name in self.SEARCH_TOOL_NAMES:
            return True

        # Check Bash commands for search patterns
        if tool_name == "Bash" and tool_input:
            command = tool_input.get("command", "")
            if not command:
                return False

            # Check cache first
            if command in self._bash_search_cache:
                return self._bash_search_cache[command]

            # Extract the first command word (more efficient than checking whole string)
            command_lower = command.lower()

            # Quick check: if none of our search commands appear anywhere, return early
            is_search = False
            if any(cmd in command_lower for cmd in self.BASH_SEARCH_COMMANDS):
                # Parse the actual command (handle pipes and complex commands)
                # Split by common delimiters
                # Match command at start or after pipe/semicolon
                parts = self.COMMAND_SPLIT_PATTERN.split(command_lower)
                for part in parts:
                    first_word = part.strip().split()[0] if part.strip() else ""
                    if first_word in self.BASH_SEARCH_COMMANDS:
                        is_search = True
                        break

            # Cache the result
            self._bash_search_cache[command] = is_search
            return is_search

        return False

    def generate_statistics(self, messages: list[dict], timezone_offset_minutes: int = 0) -> dict:
        """
        Generate comprehensive statistics from messages.

        Args:
            messages: List of processed messages
            timezone_offset_minutes: Timezone offset in minutes for local time display
                                   (e.g., -420 for PDT, 60 for CET)

        Returns:
            Dictionary containing all statistics sections:
            - overview: Project info, tokens, costs, date range
            - tools: Usage counts, error rates
            - sessions: Count, duration, error sessions
            - daily_stats: Per-day message/token counts with costs
            - hourly_pattern: Message and token distribution by hour
            - errors: Error analysis by type and category
            - models: Token usage by model
            - user_interactions: Command analysis with tool usage
            - cache: Cache hit rates and efficiency
        """
        # Extract project name from log directory path
        project_name = "Unknown Project"
        log_dir_name = Path(self.log_directory).name  # Full directory name like -Users-chip-dev-...

        if "/.claude/projects/" in self.log_directory:
            # Extract the project path part after /projects/
            project_part = self.log_directory.split("/.claude/projects/")[-1]
            # Convert hashed path back to readable format
            if project_part.startswith("-"):
                # Remove leading dash and replace remaining dashes with slashes
                project_path = project_part[1:].replace("-", "/")
                # Extract just the project name (last part of path)
                project_name = project_path.split("/")[-1]
            else:
                project_name = project_part

        stats = {
            "overview": {
                "project_name": project_name,
                "log_dir_name": log_dir_name,
                "project_path": self.log_directory,
                "total_messages": len(messages),
                "date_range": self._get_date_range(messages),
                "sessions": self._count_unique_sessions(messages),
                "message_types": dict(self.running_stats["message_counts"]),
                "total_tokens": dict(self.running_stats["tokens"]),
                "total_cost": self._calculate_total_cost(messages),
            },
            "tools": self._analyze_tools(messages),
            "sessions": self._analyze_sessions(messages),
            "daily_stats": self._calculate_daily_stats(messages, timezone_offset_minutes),
            "hourly_pattern": self._calculate_hourly_pattern(messages, timezone_offset_minutes),
            "errors": self._analyze_errors(messages),
            "models": self._analyze_models(messages),
            "user_interactions": self._analyze_user_interactions(messages),
            "cache": self._analyze_cache(messages),
        }

        return stats

    def _get_date_range(self, messages: list[dict]) -> dict:
        """Get date range of messages."""
        if not messages:
            return {"start": None, "end": None}

        timestamps = [msg["timestamp"] for msg in messages if msg["timestamp"]]
        if not timestamps:
            return {"start": None, "end": None}

        return {"start": min(timestamps), "end": max(timestamps)}

    def _count_unique_sessions(self, messages: list[dict]) -> int:
        """Count unique sessions."""
        return len({msg["session_id"] for msg in messages})

    def _count_message_types(self, messages: list[dict]) -> dict[str, int]:
        """Count messages by type."""
        counts = defaultdict(int)
        for msg in messages:
            counts[msg["type"]] += 1
        return dict(counts)

    def _sum_tokens(self, messages: list[dict]) -> dict[str, int]:
        """Sum token usage."""
        totals = defaultdict(int)
        for msg in messages:
            for key, value in msg["tokens"].items():
                totals[key] += value
        return dict(totals)

    def _analyze_tools(self, messages: list[dict]) -> dict:
        """Analyze tool usage."""
        tool_counts = dict(self.running_stats["tool_usage"])
        tool_errors = defaultdict(int)

        # Still need to calculate errors from messages
        for msg in messages:
            if msg["error"]:
                for tool in msg["tools"]:
                    tool_errors[tool["name"]] += 1

        # Calculate error rates
        error_rates = {}
        for tool, count in tool_counts.items():
            error_count = tool_errors.get(tool, 0)
            error_rates[tool] = error_count / count if count > 0 else 0

        return {"usage_counts": dict(tool_counts), "error_counts": dict(tool_errors), "error_rates": error_rates}

    def _analyze_sessions(self, messages: list[dict]) -> dict:
        """Analyze session patterns."""
        session_data = defaultdict(
            lambda: {"messages": 0, "start": None, "end": None, "tokens": defaultdict(int), "tools": set(), "errors": 0}
        )

        for msg in messages:
            session = session_data[msg["session_id"]]
            session["messages"] += 1

            # Update timestamps
            if msg["timestamp"]:
                if not session["start"] or msg["timestamp"] < session["start"]:
                    session["start"] = msg["timestamp"]
                if not session["end"] or msg["timestamp"] > session["end"]:
                    session["end"] = msg["timestamp"]

            # Sum tokens
            for key, value in msg["tokens"].items():
                session["tokens"][key] += value

            # Collect tools
            for tool in msg["tools"]:
                session["tools"].add(tool["name"])

            # Count errors
            if msg["error"]:
                session["errors"] += 1

        # Calculate session durations
        durations = []
        for session in session_data.values():
            if session["start"] and session["end"]:
                try:
                    start = datetime.fromisoformat(session["start"].replace("Z", "+00:00"))
                    end = datetime.fromisoformat(session["end"].replace("Z", "+00:00"))
                    duration = (end - start).total_seconds()
                    if duration > 0:
                        durations.append(duration)
                except (ValueError, AttributeError, TypeError):
                    pass

        avg_duration = sum(durations) / len(durations) if durations else 0

        return {
            "count": len(session_data),
            "average_duration_seconds": avg_duration,
            "average_messages": (
                sum(s["messages"] for s in session_data.values()) / len(session_data) if session_data else 0
            ),
            "sessions_with_errors": sum(1 for s in session_data.values() if s["errors"] > 0),
        }

    def _calculate_daily_stats(self, messages: list[dict], timezone_offset_minutes: int = 0) -> dict:
        """Calculate daily statistics including costs with timezone support.

        IMPORTANT: This method does NOT use self.running_stats['daily_tokens']
        because those are accumulated in UTC during processing. Instead, it
        recalculates daily statistics with the provided timezone offset to
        ensure charts display correct local dates.

        Args:
            messages: List of messages to analyze
            timezone_offset_minutes: Offset from UTC in minutes

        Returns:
            Dict with dates as keys (YYYY-MM-DD format in local time) and
            statistics including messages, tokens, sessions, costs, and interruption rates
        """
        daily = defaultdict(
            lambda: {
                "messages": 0,
                "tokens": defaultdict(int),
                "sessions": set(),
                "models": defaultdict(lambda: {"tokens": defaultdict(int), "count": 0}),
                "user_commands": 0,
                "interrupted_commands": 0,
                "errors": 0,
                "assistant_messages": 0,
            }
        )

        # Process all messages with timezone-aware dates
        for msg in messages:
            if msg["timestamp"]:
                try:
                    # Convert UTC timestamp to local date
                    from datetime import datetime, timedelta

                    utc_time = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                    local_time = utc_time + timedelta(minutes=timezone_offset_minutes)
                    date = local_time.strftime("%Y-%m-%d")

                    daily[date]["messages"] += 1
                    daily[date]["sessions"].add(msg["session_id"])

                    # Count errors and assistant messages
                    if msg["error"]:
                        daily[date]["errors"] += 1
                    if msg["type"] == "assistant":
                        daily[date]["assistant_messages"] += 1

                    # Accumulate all tokens for this day
                    for token_type, count in msg["tokens"].items():
                        daily[date]["tokens"][token_type] += count

                    # Track tokens by model for cost calculation
                    # Need all token types (including cache) broken down by model
                    if msg["type"] == "assistant" and msg["model"] != "N/A":
                        model = msg["model"]
                        daily[date]["models"][model]["count"] += 1
                        for key, value in msg["tokens"].items():
                            daily[date]["models"][model]["tokens"][key] += value
                except (KeyError, AttributeError, TypeError):
                    pass

        # Now analyze user interactions to get interruption data by day
        # Sort messages by timestamp for proper sequence analysis
        sorted_messages = sorted(messages, key=lambda x: x["timestamp"] if x["timestamp"] else "")

        # Process user commands to track interruptions by day
        for i, msg in enumerate(sorted_messages):
            if msg["type"] == "user" and not msg.get("has_tool_result", False) and msg["timestamp"]:
                try:
                    # Convert to local date
                    utc_time = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                    local_time = utc_time + timedelta(minutes=timezone_offset_minutes)
                    date = local_time.strftime("%Y-%m-%d")

                    # Check if this is an interruption message itself
                    is_interruption = self._is_interruption_message(msg["content"])

                    if not is_interruption:
                        # This is a regular user command
                        daily[date]["user_commands"] += 1

                        # Check if this command is followed by an interruption
                        followed_by_interruption = False

                        # Look for subsequent messages
                        j = i + 1
                        while j < len(sorted_messages):
                            next_msg = sorted_messages[j]

                            # Check for API error interruption
                            if (
                                next_msg["type"] == "assistant"
                                and next_msg.get("content", "").strip() == USER_INTERRUPTION_API_ERROR
                            ):
                                followed_by_interruption = True
                                break

                            # Check for user interruption in next user message
                            if next_msg["type"] == "user" and not next_msg.get("has_tool_result", False):
                                if self._is_interruption_message(next_msg["content"]):
                                    followed_by_interruption = True
                                break

                            j += 1

                        if followed_by_interruption:
                            daily[date]["interrupted_commands"] += 1

                except Exception:
                    pass

        # Convert sets to counts and calculate costs
        result = {}
        for date, data in daily.items():
            # Calculate total cost for the day
            total_cost = 0.0
            model_costs = {}

            for model, model_data in data["models"].items():
                cost_breakdown = calculate_cost(dict(model_data["tokens"]), model)
                model_costs[model] = cost_breakdown
                total_cost += cost_breakdown["total_cost"]

            # Calculate interruption rate for the day
            interruption_rate = 0.0
            if data["user_commands"] > 0:
                interruption_rate = (data["interrupted_commands"] / data["user_commands"]) * 100

            # Calculate error rate for the day
            error_rate = 0.0
            if data["assistant_messages"] > 0:
                error_rate = (data["errors"] / data["assistant_messages"]) * 100

            result[date] = {
                "messages": data["messages"],
                "sessions": len(data["sessions"]),
                "tokens": dict(data["tokens"]),
                "cost": {"total": total_cost, "by_model": model_costs},
                "user_commands": data["user_commands"],
                "interrupted_commands": data["interrupted_commands"],
                "interruption_rate": round(interruption_rate, 1),
                "errors": data["errors"],
                "assistant_messages": data["assistant_messages"],
                "error_rate": round(error_rate, 1),
            }

        return result

    def _calculate_hourly_pattern(self, messages: list[dict], timezone_offset_minutes: int = 0) -> dict:
        """Calculate hourly message and token patterns with timezone support.

        Groups messages by hour of day (0-23) in the user's local timezone.

        Args:
            messages: List of messages to analyze
            timezone_offset_minutes: Offset from UTC in minutes

        Returns:
            Dict with 'messages' and 'tokens' sub-dicts, each containing
            counts for all 24 hours (0-23 in local time)
        """
        hourly_messages = defaultdict(int)
        hourly_tokens = defaultdict(lambda: defaultdict(int))

        for msg in messages:
            if msg["timestamp"]:
                try:
                    # Convert UTC timestamp to local hour
                    from datetime import datetime, timedelta

                    utc_time = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                    local_time = utc_time + timedelta(minutes=timezone_offset_minutes)
                    hour = local_time.hour

                    hourly_messages[hour] += 1

                    # Accumulate tokens by hour
                    for token_type, count in msg["tokens"].items():
                        hourly_tokens[hour][token_type] += count
                except (KeyError, AttributeError, TypeError):
                    pass

        # Ensure all hours are represented
        return {
            "messages": {hour: hourly_messages.get(hour, 0) for hour in range(24)},
            "tokens": {
                hour: dict(hourly_tokens.get(hour, {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0}))
                for hour in range(24)
            },
        }

    def _analyze_errors(self, messages: list[dict]) -> dict:
        """Analyze error patterns."""
        error_messages = [msg for msg in messages if msg["error"]]
        # Categorize errors by type
        error_categories = defaultdict(int)

        # Collect error details with timestamps for chart
        error_details = []

        for msg in error_messages:
            # Get the error content from the message
            error_content = msg.get("content", "")

            # Add to error details if it has a timestamp
            if msg.get("timestamp"):
                error_details.append(
                    {
                        "timestamp": msg["timestamp"],
                        "session_id": msg.get("session_id", ""),
                        "model": msg.get("model", "N/A"),
                    }
                )

            # Categorize the error

            matched = False

            for category, patterns in ERROR_PATTERNS.items():
                # stop at the **first** matching category
                if any(re.search(p, error_content, re.IGNORECASE) for p in patterns):
                    error_categories[category] += 1
                    matched = True
                    break

            if not matched:
                error_categories["Other"] += 1

        # Also collect all assistant messages with timestamps for rate calculation
        # Note: Tool errors are on user messages, not assistant messages
        # So we need to look at the preceding assistant message to determine error rate
        assistant_details = []

        for i, msg in enumerate(messages):
            if msg["type"] == "assistant" and msg.get("timestamp"):
                # Check if the next message is an error
                is_error = False
                if i + 1 < len(messages):
                    next_msg = messages[i + 1]
                    is_error = next_msg["error"]

                assistant_details.append({"timestamp": msg["timestamp"], "is_error": is_error})

        return {
            "total": len(error_messages),
            "rate": len(error_messages) / len(messages) if messages else 0,
            "by_type": self._count_message_types(error_messages),
            "by_category": dict(error_categories),
            "error_details": error_details,
            "assistant_details": assistant_details,
        }

    def _analyze_models(self, messages: list[dict]) -> dict:
        """Analyze model usage."""
        model_stats = {}

        # Use running stats for model data
        for model, stats in self.running_stats["model_usage"].items():
            model_stats[model] = {
                "count": stats["count"],
                "input_tokens": stats["input_tokens"],
                "output_tokens": stats["output_tokens"],
                "cache_creation_tokens": 0,  # Still need to calculate from messages
                "cache_read_tokens": 0,  # Still need to calculate from messages
            }

        # Add cache tokens from messages
        for msg in messages:
            if msg["type"] == "assistant" and msg["model"] != "N/A":
                model = msg["model"]
                if model in model_stats:
                    model_stats[model]["cache_creation_tokens"] += msg["tokens"].get("cache_creation", 0)
                    model_stats[model]["cache_read_tokens"] += msg["tokens"].get("cache_read", 0)

        return model_stats

    def _is_interruption_message(self, content: str) -> bool:
        """Check if a message content indicates a user interruption."""
        return any(content.startswith(pattern) for pattern in USER_INTERRUPTION_PATTERNS)

    def _build_message_index(self, messages: list[dict]) -> dict:
        """Build indices for O(1) message lookups"""
        index = {
            "by_session": defaultdict(list),
            "by_type": defaultdict(list),
            "user_indices": [],
            "assistant_indices": [],
            "by_timestamp": defaultdict(list),
        }

        for i, msg in enumerate(messages):
            index["by_session"][msg["session_id"]].append(i)
            index["by_type"][msg["type"]].append(i)

            if msg["type"] == "user":
                index["user_indices"].append(i)
            elif msg["type"] == "assistant":
                index["assistant_indices"].append(i)

            if msg["timestamp"]:
                date_key = msg["timestamp"][:10]
                index["by_timestamp"][date_key].append(i)

        return index

    def _estimate_tokens(self, text: str) -> float:
        """Estimate token count from text (roughly 1 token = 4 characters)."""
        if not text:
            return 0.0
        # Claude's tokenization is roughly 4 characters per token
        return max(1.0, len(text) / 4)

    def _analyze_user_interactions(self, messages: list[dict]) -> dict:
        """Analyze user message patterns and tool usage.

        This method analyzes user commands (excluding interruptions) to determine:
        - How many tools are used per command
        - Interruption rates (commands followed by user/API interruptions)
        - Model distribution across commands
        - Assistant response steps per command

        Uses enhanced data from interaction processing when available
        (interaction_tool_count, interaction_model, interaction_assistant_steps)
        which provides more accurate counts than just counting messages.

        Returns:
            Comprehensive user interaction statistics including interruption
            rates broken down by number of tools used
        """
        # Sort messages by timestamp
        sorted_messages = sorted(messages, key=lambda x: x["timestamp"] if x["timestamp"] else "")

        # Identify real user messages (not tool results)
        real_user_messages = [
            msg for msg in messages if msg["type"] == "user" and not msg.get("has_tool_result", False)
        ]

        # Analyze user -> assistant patterns
        user_command_details = []
        user_commands = 0
        commands_with_tools = 0
        total_tools_used = 0
        total_search_tools = 0
        total_assistant_steps = 0
        tool_distribution = defaultdict(int)

        # Process each real user message
        for i, user_msg in enumerate(sorted_messages):
            if user_msg["type"] == "user" and not user_msg.get("has_tool_result", False):
                is_interruption = self._is_interruption_message(user_msg["content"])

                # Use the enhanced tool count from interaction processing if available
                interaction_tool_count = user_msg.get("interaction_tool_count", None)
                interaction_model = user_msg.get("interaction_model", "N/A")
                interaction_assistant_steps = user_msg.get("interaction_assistant_steps", None)

                # Find all assistant responses to this user message
                # Look for assistant messages after this user message until the next user message
                j = i + 1
                assistant_responses = []
                tools_used = []
                api_error_found = False

                while j < len(sorted_messages):
                    msg = sorted_messages[j]

                    # Stop at next real user message (not tool result)
                    if msg["type"] == "user" and not msg.get("has_tool_result", False):
                        break

                    # Collect assistant messages
                    if msg["type"] == "assistant":
                        # Check for API error
                        if msg.get("content", "").strip() == USER_INTERRUPTION_API_ERROR:
                            api_error_found = True
                        assistant_responses.append(msg)
                        # Track tools used
                        if msg.get("tools"):
                            tools_used.extend(msg["tools"])

                    j += 1

                # Get the model - prefer interaction model over first assistant response
                model_used = interaction_model if interaction_model != "N/A" else "N/A"
                if model_used == "N/A" and assistant_responses:
                    model_used = assistant_responses[0].get("model", "N/A")

                # Use enhanced tool count if available
                if interaction_tool_count is not None:
                    # Use the reconciled tool count from interaction processing
                    actual_tool_count = interaction_tool_count
                    # Create synthetic tool list if we don't have details but know count
                    if actual_tool_count > 0 and not tools_used:
                        tools_used = [{"name": "Unknown"} for _ in range(actual_tool_count)]
                else:
                    # Fallback to counting tools from messages
                    actual_tool_count = len(tools_used)

                followed_by_interruption = api_error_found

                # If not already marked as interrupted, check messages after assistant responses
                if not followed_by_interruption:
                    k = j  # Start from where we left off
                    while k < len(sorted_messages):
                        next_msg = sorted_messages[k]

                        # Check if it's an API error assistant message
                        if (
                            next_msg["type"] == "assistant"
                            and next_msg.get("content", "").strip() == USER_INTERRUPTION_API_ERROR
                        ):
                            followed_by_interruption = True
                            break

                        # Check if it's a user interruption message
                        if next_msg["type"] == "user" and not next_msg.get("has_tool_result", False):
                            # Found the next user message
                            followed_by_interruption = self._is_interruption_message(next_msg["content"])
                            break

                        k += 1

                # Extract actual tool names for display
                tool_names = []
                if tools_used:
                    # For reconciled tool counts, only take the first N tools
                    if interaction_tool_count is not None and interaction_tool_count < len(tools_used):
                        # Take only the number of tools indicated by the reconciled count
                        tool_names = [tool.get("name", "Unknown") for tool in tools_used[:interaction_tool_count]]
                    else:
                        tool_names = [tool.get("name", "Unknown") for tool in tools_used]

                # Use deduplicated assistant steps count if available
                actual_assistant_steps = (
                    interaction_assistant_steps if interaction_assistant_steps is not None else len(assistant_responses)
                )

                # Estimate tokens for this command
                estimated_tokens = self._estimate_tokens(user_msg["content"])

                # Count search tools - optimized batch processing
                search_tools_in_command = 0
                tools_to_check = tools_used[:actual_tool_count] if actual_tool_count < len(tools_used) else tools_used

                # Fast path: check direct search tools first (most common)
                for tool in tools_to_check:
                    tool_name = tool.get("name", "")
                    if tool_name in self.SEARCH_TOOL_NAMES:
                        search_tools_in_command += 1
                    elif tool_name == "Bash":
                        # Only do expensive bash check if needed
                        tool_input = tool.get("input", {})
                        if self._is_search_tool(tool_name, tool_input):
                            search_tools_in_command += 1

                # Store command details (for ALL commands, including interruptions)
                user_command_details.append(
                    {
                        "user_message": user_msg["content"],
                        "user_message_truncated": (
                            user_msg["content"][:100] + "..." if len(user_msg["content"]) > 100 else user_msg["content"]
                        ),
                        "timestamp": user_msg["timestamp"],
                        "session_id": user_msg["session_id"],
                        "tools_used": actual_tool_count,  # Use reconciled count
                        "tool_names": tool_names,
                        "has_tools": actual_tool_count > 0,
                        "assistant_steps": actual_assistant_steps,  # Use deduplicated count
                        "model": model_used,
                        "is_interruption": is_interruption,
                        "followed_by_interruption": followed_by_interruption,
                        "estimated_tokens": estimated_tokens,
                        "search_tools_used": search_tools_in_command,
                    }
                )

                # Only count non-interruption commands for statistics
                if not is_interruption:
                    user_commands += 1
                    total_assistant_steps += actual_assistant_steps
                    total_search_tools += search_tools_in_command

                    if actual_tool_count > 0:
                        commands_with_tools += 1
                        total_tools_used += actual_tool_count
                        tool_distribution[actual_tool_count] += 1
                    else:
                        tool_distribution[0] += 1

        # Calculate user interruption rate
        # Interruption Rate = (Commands followed by interruptions / Total non-interruption commands) × 100%
        #
        # Where:
        # - N = Total non-interruption user commands
        # - M = Commands followed by interruptions (either user or API interruptions)
        # - Rate = (M / N) × 100%
        #
        # Example: If there are 100 user commands and 5 are followed by interruptions,
        # the interruption rate is 5%, meaning 5% of user commands required manual
        # intervention in the subsequent assistant response.
        non_interruption_commands = 0
        commands_followed_by_interruption = 0

        # Calculate interruption rate by number of tools used
        # Track: for commands using N tools, how many are followed by interruptions
        #
        # Note: Interruptions after 0-tool commands can occur when:
        # 1. Assistant was about to use tools but was interrupted before starting
        # 2. Assistant completed initial response, then started tool use which was interrupted
        # 3. The interruption prevented proper logging of tool usage
        # This is why we include all commands in the analysis, not just those with tools
        interruption_by_tool_count = defaultdict(lambda: {"total": 0, "interrupted": 0})

        for _, cmd in enumerate(user_command_details):
            if not cmd["is_interruption"]:
                non_interruption_commands += 1
                tools_used = cmd["tools_used"]
                interruption_by_tool_count[tools_used]["total"] += 1

                # Check if this command is followed by an interruption (user or API)
                if cmd.get("followed_by_interruption", False):
                    commands_followed_by_interruption += 1
                    interruption_by_tool_count[tools_used]["interrupted"] += 1

        # Calculate interruption rate: M / N
        interruption_rate = (
            (commands_followed_by_interruption / non_interruption_commands * 100)
            if non_interruption_commands > 0
            else 0
        )

        # Calculate interruption rates for each tool count
        tool_interruption_rates = {}
        for tool_count, data in interruption_by_tool_count.items():
            if data["total"] > 0:
                rate = (data["interrupted"] / data["total"]) * 100
                tool_interruption_rates[tool_count] = {
                    "rate": round(rate, 1),
                    "total_commands": data["total"],
                    "interrupted_commands": data["interrupted"],
                }

        # Calculate model distribution for non-interruption commands
        model_distribution = defaultdict(int)
        for cmd in user_command_details:
            if not cmd["is_interruption"]:
                model = cmd.get("model", "N/A")
                if model != "N/A":
                    model_distribution[model] += 1

        # Calculate average tokens per command (excluding interruptions)
        total_command_tokens = sum(
            cmd["estimated_tokens"] for cmd in user_command_details if not cmd["is_interruption"]
        )
        avg_tokens_per_command = total_command_tokens / user_commands if user_commands > 0 else 0

        # Calculate percentages and averages
        pct_requiring_tools = (commands_with_tools / user_commands * 100) if user_commands > 0 else 0
        avg_tools_per_command = total_tools_used / user_commands if user_commands > 0 else 0
        avg_tools_when_used = total_tools_used / commands_with_tools if commands_with_tools > 0 else 0
        avg_steps_per_command = total_assistant_steps / user_commands if user_commands > 0 else 0
        # Count non-interruption commands that have tools
        non_interruption_commands_with_tools = sum(
            1 for cmd in user_command_details if cmd["has_tools"] and not cmd["is_interruption"]
        )
        pct_steps_with_tools = (non_interruption_commands_with_tools / user_commands * 100) if user_commands > 0 else 0

        # Calculate search tool percentage
        search_percentage = (total_search_tools / total_tools_used * 100) if total_tools_used > 0 else 0

        return {
            "real_user_messages": len(real_user_messages),
            "user_commands_analyzed": user_commands,
            "commands_requiring_tools": commands_with_tools,
            "commands_without_tools": user_commands - commands_with_tools,
            "percentage_requiring_tools": round(pct_requiring_tools, 1),
            "total_tools_used": total_tools_used,
            "total_search_tools": total_search_tools,
            "search_tool_percentage": round(search_percentage, 1),
            "total_assistant_steps": total_assistant_steps,
            "avg_tools_per_command": round(avg_tools_per_command, 2),
            "avg_tools_when_used": round(avg_tools_when_used, 2),
            "avg_steps_per_command": round(avg_steps_per_command, 2),
            "avg_tokens_per_command": round(avg_tokens_per_command, 1),
            "percentage_steps_with_tools": round(pct_steps_with_tools, 1),
            "tool_count_distribution": dict(tool_distribution),
            "command_details": user_command_details,
            # User interruption metrics
            "interruption_rate": round(interruption_rate, 1),
            "non_interruption_commands": non_interruption_commands,
            "commands_followed_by_interruption": commands_followed_by_interruption,
            # Interruption rate by tool count
            "tool_interruption_rates": tool_interruption_rates,
            # Model distribution
            "model_distribution": dict(model_distribution),
        }

    def _analyze_cache(self, messages: list[dict]) -> dict:
        """Analyze cache usage patterns."""
        # Initialize counters
        total_cache_created = 0
        total_cache_read = 0
        messages_with_cache_read = 0
        messages_with_cache_created = 0
        assistant_messages = 0

        # Calculate cache metrics
        for msg in messages:
            if msg["type"] == "assistant":
                assistant_messages += 1

                # Check cache usage
                cache_created = msg["tokens"].get("cache_creation", 0)
                cache_read = msg["tokens"].get("cache_read", 0)

                if cache_created > 0:
                    messages_with_cache_created += 1
                    total_cache_created += cache_created

                if cache_read > 0:
                    messages_with_cache_read += 1
                    total_cache_read += cache_read

        # Calculate cache hit rate
        cache_hit_rate = (messages_with_cache_read / assistant_messages * 100) if assistant_messages > 0 else 0

        # Calculate token savings
        # Cache creation costs 25% more than base price, cache read costs 90% less
        # So for every token read from cache instead of fresh processing:
        # - Fresh processing would cost: 1.0 token units
        # - Cache read costs: 0.1 token units
        # - Savings per cached token: 0.9 token units
        # But we had to create the cache first at 1.25 token units
        tokens_saved = total_cache_read - total_cache_created  # Net tokens saved

        # Calculate cost savings in base token units
        # Cache creation cost: tokens * 1.25
        # Cache read cost: tokens * 0.10
        # Fresh processing cost: tokens * 1.00
        cache_creation_cost = total_cache_created * 1.25
        cache_read_cost = total_cache_read * 0.10
        fresh_processing_cost = total_cache_read * 1.00  # What it would have cost without cache

        cost_saved = fresh_processing_cost - cache_read_cost - cache_creation_cost

        # Calculate cache efficiency (existing metric)
        cache_efficiency = (total_cache_read / total_cache_created * 100) if total_cache_created > 0 else 0

        # Calculate break-even point
        # Break-even is when cache reads exceed cache creation (since reads are 90% cheaper)
        break_even_achieved = total_cache_read > total_cache_created

        return {
            "total_created": total_cache_created,
            "total_read": total_cache_read,
            "messages_with_cache_read": messages_with_cache_read,
            "messages_with_cache_created": messages_with_cache_created,
            "assistant_messages": assistant_messages,
            "hit_rate": round(cache_hit_rate, 1),
            "efficiency": round(min(100, cache_efficiency), 1),
            "tokens_saved": tokens_saved,
            "cost_saved_base_units": round(cost_saved, 2),
            "break_even_achieved": break_even_achieved,
            "cache_roi": round((total_cache_read / total_cache_created - 1) * 100, 1) if total_cache_created > 0 else 0,
        }

    def _calculate_total_cost(self, messages: list[dict]) -> float:
        """Calculate total project cost based on all messages."""
        total_cost = 0.0
        model_tokens = defaultdict(lambda: {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0})

        # Aggregate tokens by model
        for msg in messages:
            if msg["type"] == "assistant" and msg["model"] and msg["model"] != "N/A":
                model = msg["model"]
                model_tokens[model]["input"] += msg["tokens"].get("input", 0)
                model_tokens[model]["output"] += msg["tokens"].get("output", 0)
                model_tokens[model]["cache_creation"] += msg["tokens"].get("cache_creation", 0)
                model_tokens[model]["cache_read"] += msg["tokens"].get("cache_read", 0)

        # Calculate cost for each model
        for model, tokens in model_tokens.items():
            cost = calculate_cost(tokens, model)
            total_cost += cost["total_cost"]

        return total_cost

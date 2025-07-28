#!/usr/bin/env python3
"""
Claude log processor with advanced message deduplication and statistics generation.

This module handles:
- Streaming message deduplication (merging messages with same ID)
- Cross-session deduplication (removing duplicate messages from continued conversations)
- Message type classification (user, assistant, task, summary, compact_summary)
- Tool usage extraction and tracking
- Statistics generation via the StatisticsGenerator class
- Timezone-aware processing for accurate local time display
"""

import glob
import hashlib
import logging
import os
import re
from collections import defaultdict
from typing import Any

import orjson  # Faster JSON parsing

from .stats import StatisticsGenerator

logger = logging.getLogger(__name__)


class Interaction:
    """Represents a complete user-assistant interaction."""

    def __init__(self, user_message: dict):
        self.user_message = user_message
        self.assistant_messages = []
        self.tool_results = []
        self.is_complete = False
        self.session_id = user_message.get("session_id", "")
        self.start_time = user_message.get("timestamp", "")
        self.end_time = user_message.get("timestamp", "")
        self.interaction_id = self._generate_id()
        self.is_continuation = False
        self.previous_command = None
        self.model = "N/A"
        self.tools_used = []
        self.final_tool_count = 0
        self.has_task_tool = False

    def _generate_id(self) -> str:
        """Generate unique interaction ID."""
        content = self._extract_content(self.user_message)
        content_hash = hashlib.md5(content.encode()).hexdigest()[:8]
        return f"{self.start_time}:{content_hash}"

    def _extract_content(self, msg: dict) -> str:
        """Extract text content from message."""
        content_parts = []

        # First check if content is already a string (processed message)
        if isinstance(msg.get("content"), str):
            return msg["content"]

        # Otherwise extract from message structure
        if msg.get("message", {}).get("content"):
            content_list = msg["message"]["content"]
            if isinstance(content_list, list):
                for part in content_list:
                    if isinstance(part, dict) and part.get("type") == "text":
                        content_parts.append(part.get("text", ""))
            elif isinstance(content_list, str):
                return content_list

        return " ".join(content_parts)

    def add_assistant_message(self, msg: dict):
        """Add an assistant message to this interaction."""
        self.assistant_messages.append(msg)
        if msg.get("timestamp"):
            self.end_time = msg["timestamp"]

        # Update model
        if self.model == "N/A":
            if msg.get("model") and msg["model"] != "N/A":
                self.model = msg["model"]
            elif msg.get("_raw_data", {}).get("message", {}).get("model"):
                self.model = msg["_raw_data"]["message"]["model"]

        # Extract tools from raw data if available
        raw_data = msg.get("_raw_data", {})
        if raw_data and raw_data.get("message", {}).get("content"):
            content_list = raw_data["message"]["content"]
            if isinstance(content_list, list):
                for content in content_list:
                    if isinstance(content, dict) and content.get("type") == "tool_use":
                        tool_info = {
                            "name": content.get("name", ""),
                            "id": content.get("id", ""),
                            "input": content.get("input", {}),
                        }
                        self.tools_used.append(tool_info)

                        # Check for Task tool
                        if content.get("name") == "Task":
                            self.has_task_tool = True

        # Fallback to processed tools if no raw data
        elif msg.get("tools"):
            for tool in msg["tools"]:
                self.tools_used.append(tool)
                if tool.get("name") == "Task":
                    self.has_task_tool = True

    def add_tool_result(self, msg: dict):
        """Add a tool result message."""
        self.tool_results.append(msg)
        if msg.get("timestamp"):
            self.end_time = msg["timestamp"]

    def completeness_score(self) -> int:
        """Calculate how complete this interaction is."""
        score = 0

        # Has assistant response
        if self.assistant_messages:
            score += 100

        # Has model info
        if self.model != "N/A":
            score += 50

        # Has tools
        score += len(self.tools_used) * 10

        # Has tool results
        score += len(self.tool_results) * 5

        # Has output tokens
        total_output = sum(
            msg.get("message", {}).get("usage", {}).get("output_tokens", 0) for msg in self.assistant_messages
        )
        score += min(total_output, 1000)  # Cap at 1000

        return score

    def has_complete_response(self) -> bool:
        """Check if this interaction has a complete response."""
        if not self.assistant_messages:
            return False

        # Check for stop reason indicating completion
        for msg in self.assistant_messages:
            stop_reason = msg.get("message", {}).get("stop_reason", "")
            if stop_reason in ["end_turn", "stop_sequence", "tool_use"]:
                return True

        return False

    def merge_tools_from(self, other: "Interaction"):
        """Merge tools from another interaction, avoiding duplicates."""
        seen_tool_ids = {tool.get("id") for tool in self.tools_used if tool.get("id")}

        for tool in other.tools_used:
            tool_id = tool.get("id")
            if tool_id and tool_id not in seen_tool_ids:
                self.tools_used.append(tool)
                seen_tool_ids.add(tool_id)

                # Check for Task tool
                if tool.get("name") == "Task":
                    self.has_task_tool = True


class ClaudeLogProcessor:
    """
    Processes Claude log files to extract and deduplicate messages.

    Features:
    - Handles streaming message deduplication (multiple entries with same message ID)
    - Removes cross-session duplicates from conversation continuations
    - Classifies messages by type: user, assistant, task, summary, compact_summary
    - Extracts tool usage and tracks statistics
    - Supports timezone-aware processing for accurate local time display

    The processor accumulates running statistics during processing for efficiency,
    then generates comprehensive statistics via the StatisticsGenerator class.
    """

    def __init__(self, log_directory: str):
        self.log_directory = log_directory
        self.messages = []
        self.statistics = defaultdict(lambda: defaultdict(int))
        # Running statistics accumulated during processing
        self.running_stats = {
            "tokens": defaultdict(int),
            "message_counts": defaultdict(int),
            "tool_usage": defaultdict(int),
            "daily_tokens": defaultdict(lambda: defaultdict(int)),  # UTC-based, will be recalculated with timezone
            "model_usage": defaultdict(lambda: defaultdict(int)),
        }

    def _update_running_stats(self, message: dict):
        """Update running statistics as we process messages.

        Note: daily_tokens are accumulated in UTC and will be recalculated
        with timezone offset in the StatisticsGenerator.
        """
        # Message type counts
        self.running_stats["message_counts"][message["type"]] += 1

        # Token accumulation
        for token_type, count in message["tokens"].items():
            self.running_stats["tokens"][token_type] += count

        # Daily accumulation
        if message["timestamp"]:
            date = message["timestamp"][:10]
            self.running_stats["daily_tokens"][date]["input"] += message["tokens"]["input"]
            self.running_stats["daily_tokens"][date]["output"] += message["tokens"]["output"]
            self.running_stats["daily_tokens"][date]["cache_creation"] += message["tokens"]["cache_creation"]
            self.running_stats["daily_tokens"][date]["cache_read"] += message["tokens"]["cache_read"]

        # Tool usage
        for tool in message["tools"]:
            self.running_stats["tool_usage"][tool["name"]] += 1

        # Model usage
        if message.get("model") and message["model"] != "N/A":
            self.running_stats["model_usage"][message["model"]]["count"] += 1
            self.running_stats["model_usage"][message["model"]]["input_tokens"] += message["tokens"]["input"]
            self.running_stats["model_usage"][message["model"]]["output_tokens"] += message["tokens"]["output"]

    def process_logs(self, limit: int | None = None, timezone_offset_minutes: int = 0) -> tuple[list[dict], dict]:
        """Process all log files and extract messages with comprehensive statistics.

        This method performs:
        1. Session continuation detection
        2. Message extraction from JSONL files
        3. Streaming message deduplication (merging by message ID)
        4. Cross-session deduplication
        5. User interaction reconciliation
        6. Statistics generation with timezone support

        Args:
            limit: Optional limit on number of messages to return
            timezone_offset_minutes: Timezone offset for local time display (e.g., -420 for PDT)

        Returns:
            Tuple of (messages, statistics) where messages are sorted by timestamp
        """
        files = sorted(glob.glob(os.path.join(self.log_directory, "*.jsonl")))

        # Initialize statistics tracking (for Phase 1 optimizations)
        self.statistics = {
            "files_processed": 0,
            "messages_extracted": 0,
            "errors": 0,
            "summary": {"count": 0, "compact": 0},
        }
        self.running_stats = {
            "tokens": defaultdict(int),
            "message_counts": defaultdict(int),
            "tool_usage": defaultdict(int),
            "daily_tokens": defaultdict(lambda: defaultdict(int)),
            "model_usage": defaultdict(lambda: {"count": 0, "input_tokens": 0, "output_tokens": 0}),
        }
        self.message_index = None

        # Phase 1: Detect session continuations
        continuations = self._detect_session_continuations(files)

        # Phase 2: Load and process all messages
        all_messages = []
        session_metadata = {}

        for file_index, file_path in enumerate(files):
            session_id = os.path.basename(file_path).replace(".jsonl", "")
            session_messages = []
            self._process_file(file_path, session_id, session_messages)

            # Add session metadata
            session_metadata[session_id] = {
                "index": file_index,
                "file_path": file_path,
                "message_count": len(session_messages),
                "continues_from": continuations.get(session_id),
            }

            all_messages.extend(session_messages)

        # Phase 3: Combined streaming merge and deduplication
        merged_messages = self._merge_and_deduplicate_streaming(all_messages)

        # Phase 4: Separate summaries from regular messages
        summary_messages = []
        regular_messages = []
        for msg in merged_messages:
            if msg.get("type") in ["summary", "compact_summary"]:
                summary_messages.append(msg)
            else:
                regular_messages.append(msg)

        # Phase 5: Group regular messages into interactions
        interactions = self._group_into_interactions(regular_messages, session_metadata)

        # Phase 6: Handle split interactions across files
        self._handle_split_interactions(interactions, session_metadata)

        # Phase 7: Merge duplicate interactions
        merged_interactions = self._merge_duplicate_interactions(interactions)

        # Phase 8: Reconcile tool counts
        self._reconcile_all_tool_counts(merged_interactions)

        # Phase 9: Convert back to messages for compatibility
        interaction_messages = self._interactions_to_messages(merged_interactions)

        # Phase 10: Combine interaction messages with summaries
        all_messages = interaction_messages + summary_messages

        # Phase 11: Deduplicate summaries (they can appear in multiple sessions)
        final_messages = self._deduplicate_all_messages(all_messages)

        # Phase 12: Sort and limit
        final_messages.sort(key=lambda x: x["timestamp"] if x["timestamp"] else "", reverse=True)
        if limit:
            final_messages = final_messages[:limit]

        # Phase 13: Generate statistics
        # Reset message counts since we need counts after deduplication
        self.running_stats["message_counts"] = defaultdict(int)
        for msg in final_messages:
            self.running_stats["message_counts"][msg["type"]] += 1


        # Use the StatisticsGenerator for all statistics
        stats_generator = StatisticsGenerator(self.log_directory, self.running_stats)
        statistics = stats_generator.generate_statistics(final_messages, timezone_offset_minutes)

        return final_messages, statistics

    def _process_file(self, file_path: str, session_id: str, raw_messages: list[dict]):
        """Process a single log file and extract messages.

        Handles different message types:
        - Regular messages (user, assistant)
        - Task messages (isSidechain=true)
        - Summary messages (type='summary')
        - Compact summaries (isCompactSummary=true)

        Args:
            file_path: Path to the JSONL file
            session_id: Extracted session ID from filename
            raw_messages: List to append extracted messages to
        """
        last_model_seen = "N/A"  # Track last model in session for summaries

        with open(file_path, "rb") as f:  # Binary mode for orjson
            for line_num, line in enumerate(f, 1):
                try:
                    data = orjson.loads(line)

                    # Process summary entries
                    if data.get("type") == "summary":
                        self.statistics["summary"]["count"] += 1
                        summary_message = self._extract_summary(data, session_id)
                        if summary_message:
                            # Try to infer timestamp from previous message
                            if raw_messages and raw_messages[-1].get("timestamp"):
                                summary_message["timestamp"] = raw_messages[-1]["timestamp"]
                            # Use last seen model
                            summary_message["model"] = last_model_seen
                            raw_messages.append(summary_message)
                            self._update_running_stats(summary_message)
                        continue

                    # Process compact summaries
                    if data.get("isCompactSummary"):
                        self.statistics["summary"]["compact"] += 1
                        # These are user messages, so process normally but add a tag
                        message = self._extract_message(data, session_id)
                        if message:
                            message["type"] = "compact_summary"  # Override type for filtering
                            message["_raw_data"] = data  # Preserve raw data
                            # Use last seen model for compact summaries
                            if last_model_seen != "N/A":
                                message["model"] = last_model_seen
                            raw_messages.append(message)
                            self._update_running_stats(message)
                        continue

                    # Process regular message entries
                    if "message" in data and "type" in data:
                        message = self._extract_message(data, session_id)
                        if message:
                            message["_raw_data"] = data  # Preserve raw data
                            # Track model for assistant messages
                            if message["type"] == "assistant" and message.get("model") and message["model"] != "N/A":
                                last_model_seen = message["model"]
                            raw_messages.append(message)
                            self._update_running_stats(message)

                except Exception as e:
                    logger.info(f"Error processing line {line_num} in {file_path}: {e}")

    def _extract_summary(self, data: dict, session_id: str) -> dict | None:
        """Extract summary entry data."""
        return {
            "session_id": session_id,
            "type": "summary",
            "timestamp": data.get("timestamp", ""),
            "model": "N/A",
            "content": data.get("summary", ""),
            "tools": [],
            "tokens": {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0},
            "cwd": "",
            "uuid": data.get("uuid", ""),
            "parent_uuid": None,
            "is_sidechain": False,
            "has_tool_result": False,
            "error": False,
            "is_interruption": False,
            "leaf_uuid": data.get("leafUuid", ""),
        }

    def _extract_message(self, data: dict, session_id: str) -> dict | None:
        """Extract message data from log entry.

        Performs message type classification:
        - If isSidechain=true and type='user', classify as 'task'
        - Otherwise use the root type field

        Also extracts:
        - Message content (text and tool uses)
        - Token usage information
        - Tool results if present
        - Message ID for assistant messages (used for streaming deduplication)
        """
        if not isinstance(data.get("message"), dict):
            return None

        # Determine message type
        msg_type = data["type"]
        is_sidechain = data.get("isSidechain", False)

        # Classify message type: Task tool invocations have isSidechain=true
        if is_sidechain and msg_type == "user":
            msg_type = "task"

        message = {
            "session_id": session_id,
            "type": msg_type,
            "timestamp": data.get("timestamp", ""),
            "model": data["message"].get("model", "N/A"),
            "content": "",
            "tools": [],
            "tokens": {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0},
            "cwd": data.get("cwd", ""),
            "uuid": data.get("uuid", ""),
            "parent_uuid": data.get("parentUuid"),
            "is_sidechain": is_sidechain,
            "has_tool_result": False,
            "error": False,
        }

        # Add message ID for assistant messages
        if "id" in data["message"]:
            message["message_id"] = data["message"]["id"]

        # Extract content and tools
        self._extract_content(data, message)

        # Extract token usage
        self._extract_tokens(data, message)

        # Check for tool results
        if "toolUseResult" in data:
            message["has_tool_result"] = True
            self._process_tool_result(data["toolUseResult"], message)

        return message

    def _extract_content(self, data: dict, message: dict):
        """Extract content from message."""
        content_parts = []
        content_data = data["message"].get("content", [])

        if isinstance(content_data, str):
            content_parts.append(content_data)
        elif isinstance(content_data, list):
            for item in content_data:
                if isinstance(item, dict):
                    item_type = item.get("type")

                    if item_type == "text":
                        content_parts.append(item.get("text", ""))

                    elif item_type == "tool_use":
                        tool_info = {
                            "name": item.get("name", "unknown"),
                            "input": item.get("input", {}),
                            "id": item.get("id", ""),
                        }
                        message["tools"].append(tool_info)

                    elif item_type == "tool_result":
                        result = item.get("content", "")
                        is_error = item.get("is_error", False)

                        if is_error:
                            message["error"] = True

                        if result:
                            prefix = "[Tool Error: " if is_error else "[Tool Result: "
                            content_parts.append(f"{prefix}{result}]")
                        else:
                            content_parts.append("[Tool Result: Empty/Success]")

                elif isinstance(item, str):
                    content_parts.append(item)

        # Join content parts
        message["content"] = "\n".join(content_parts)

        # Generate tool summary if no text content
        if not message["content"] and message["tools"]:
            message["content"] = self._generate_tool_summary(message["tools"])

    def _generate_tool_summary(self, tools: list[dict]) -> str:
        """Generate a summary of tool uses."""
        summaries = []

        for tool in tools:
            name = tool["name"]
            inputs = tool["input"]

            if name == "MultiEdit":
                edits = inputs.get("edits", [])
                summaries.append(f"Used {name} to make {len(edits)} edits")

            elif name in ["Read", "Write", "Edit"]:
                file_path = inputs.get("file_path", "unknown")
                # Shorten long paths
                if len(file_path) > 50:
                    file_path = "..." + file_path[-47:]
                summaries.append(f"Used {name} on {file_path}")

            elif name == "Bash":
                command = inputs.get("command", "")[:50]
                if len(inputs.get("command", "")) > 50:
                    command += "..."
                summaries.append(f"Used {name}: {command}")

            elif name == "Task":
                desc = inputs.get("description", "task")[:30]
                summaries.append(f"Used {name}: {desc}")

            else:
                summaries.append(f"Used {name}")

        return " | ".join(summaries)

    def _extract_tokens(self, data: dict, message: dict):
        """Extract token usage information."""
        usage = data["message"].get("usage", {})

        if usage:
            message["tokens"]["input"] = usage.get("input_tokens", 0)
            message["tokens"]["output"] = usage.get("output_tokens", 0)
            message["tokens"]["cache_creation"] = usage.get("cache_creation_input_tokens", 0)
            message["tokens"]["cache_read"] = usage.get("cache_read_input_tokens", 0)

    def _process_tool_result(self, tool_result: Any, message: dict):
        """Process tool result data."""
        result_parts = []

        if isinstance(tool_result, str):
            # Simple string result
            truncated = tool_result[:200] + "..." if len(tool_result) > 200 else tool_result
            result_parts.append(truncated)

        elif isinstance(tool_result, dict):
            # Structured result
            if "filePath" in tool_result:
                result_parts.append(f"File: {tool_result['filePath']}")

            if "stdout" in tool_result and tool_result["stdout"]:
                stdout = tool_result["stdout"][:100]
                result_parts.append(f"Output: {stdout}{'...' if len(tool_result['stdout']) > 100 else ''}")

            if tool_result.get("interrupted"):
                result_parts.append("Interrupted by user")

        # Add to content
        if result_parts:
            detail_text = " | ".join(result_parts)
            if message["content"]:
                message["content"] += f"\n[Details] {detail_text}"
            else:
                message["content"] = f"[Tool Execution Result] {detail_text}"

    def _merge_message_group(self, group: list[dict]) -> dict:
        """Merge a group of streaming messages with the same message ID.

        Claude logs streaming responses as multiple entries. This method:
        - Combines all tool uses (deduplicating by tool ID)
        - Merges text content (avoiding duplicates)
        - Uses the latest timestamp from the group
        - Sums token counts across all messages in the group
        - Preserves raw data with merged content arrays

        Args:
            group: List of messages with the same message ID

        Returns:
            Single merged message containing all content and tools
        """
        # Start with first message
        merged = group[0].copy()

        # Collect all content and tools
        all_tools = []
        text_parts = []

        for msg in group:
            # Collect tools
            for tool in msg["tools"]:
                # Check if tool already exists by ID
                tool_id = tool.get("id")
                if tool_id:
                    if not any(t.get("id") == tool_id for t in all_tools):
                        all_tools.append(tool)
                else:
                    # No ID, just check if not duplicate
                    if tool not in all_tools:
                        all_tools.append(tool)

            # Collect text content
            if msg["content"] and not msg["content"].startswith("Used "):
                # Only add if not already in text_parts (avoid duplicates)
                if msg["content"] not in text_parts:
                    text_parts.append(msg["content"])

        # Update merged message
        merged["tools"] = all_tools

        # Combine text content
        if text_parts:
            merged["content"] = "\n".join(text_parts)
        elif all_tools:
            merged["content"] = self._generate_tool_summary(all_tools)
        else:
            merged["content"] = ""

        # Calculate timing information
        timestamps = [msg["timestamp"] for msg in group if msg.get("timestamp")]
        if timestamps:
            start_time = min(timestamps)
            end_time = max(timestamps)
            merged["start_timestamp"] = start_time
            merged["timestamp"] = end_time

            # Calculate duration in milliseconds
            try:
                from datetime import datetime

                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
                merged["duration_ms"] = duration_ms
            except (ValueError, AttributeError, TypeError):
                merged["duration_ms"] = 0
        else:
            # Fallback to latest timestamp only
            merged["timestamp"] = max(msg["timestamp"] for msg in group)
            merged["start_timestamp"] = merged["timestamp"]
            merged["duration_ms"] = 0

        # Preserve raw data if tools were found
        if all_tools and group[0].get("_raw_data"):
            # Merge raw data content arrays
            raw_content = []
            seen_tool_ids = set()

            for msg in group:
                if msg.get("_raw_data", {}).get("message", {}).get("content"):
                    for content in msg["_raw_data"]["message"]["content"]:
                        if content.get("type") == "tool_use":
                            tool_id = content.get("id")
                            if tool_id and tool_id not in seen_tool_ids:
                                raw_content.append(content)
                                seen_tool_ids.add(tool_id)
                        elif content.get("type") == "text":
                            # Add text content if not duplicate
                            if content not in raw_content:
                                raw_content.append(content)

            # Update raw data with merged content
            if "_raw_data" not in merged:
                merged["_raw_data"] = group[0]["_raw_data"].copy()
            merged["_raw_data"]["message"]["content"] = raw_content

        # Sum tokens from ALL messages in the group (not just group[1:])
        # Reset tokens to sum them properly
        merged["tokens"] = {"input": 0, "output": 0, "cache_creation": 0, "cache_read": 0}

        for msg in group:
            for key in ["input", "output", "cache_creation", "cache_read"]:
                merged["tokens"][key] += msg["tokens"][key]

        return merged

    def _deduplicate_messages(self, messages: list[dict]) -> list[dict]:
        """Remove duplicate messages across sessions.

        When conversations are continued, messages are copied to new sessions
        with identical content and timestamps. We deduplicate based on:
        - Message type
        - Exact timestamp
        - Content preview (first 500 chars)
        - UUID if available

        Note: This does NOT use session_id in the deduplication key,
        allowing cross-session duplicate removal.
        """
        seen = set()
        deduped = []

        for msg in messages:
            # Create deduplication key without session_id
            # Use full content for better accuracy since we're matching exact duplicates
            content_key = msg["content"][:500] if msg["content"] else ""
            timestamp_key = msg["timestamp"] if msg["timestamp"] else ""

            # Include UUID if available for more accurate deduplication
            uuid_key = msg.get("uuid", "")

            # Create a unique key for this message
            key = f"{msg['type']}:{timestamp_key}:{content_key}:{uuid_key}"

            if key not in seen:
                seen.add(key)
                deduped.append(msg)

        return deduped

    def _deduplicate_all_messages(self, messages: list[dict]) -> list[dict]:
        """Remove duplicate messages including summaries across sessions.

        Summaries and compact summaries can appear in multiple log files
        with the same timestamp and content when sessions are continued.

        Special handling for summaries:
        - Uses shorter content preview (200 chars) for summaries
        - Applies same deduplication logic to compact_summary type
        """
        seen = set()
        deduped = []

        for msg in messages:
            # Special handling for summaries and compact summaries
            if msg["type"] in ["summary", "compact_summary"]:
                # For summaries, deduplicate based on type, timestamp, and content
                key = f"{msg['type']}:{msg.get('timestamp', '')}:{msg.get('content', '')[:200]}"
            else:
                # For regular messages, use the existing deduplication logic
                content_key = msg["content"][:500] if msg["content"] else ""
                timestamp_key = msg["timestamp"] if msg["timestamp"] else ""
                uuid_key = msg.get("uuid", "")
                key = f"{msg['type']}:{timestamp_key}:{content_key}:{uuid_key}"

            if key not in seen:
                seen.add(key)
                deduped.append(msg)

        return deduped

    def _single_pass_deduplication(self, messages: list[dict]) -> list[dict]:
        """Single-pass deduplication that combines streaming merge and deduplication.

        This is Phase 2 optimization that combines:
        1. Streaming message deduplication (by message ID)
        2. General deduplication (by content key)
        """
        streaming_groups = defaultdict(list)
        non_streaming = []

        # First pass: Separate streaming messages from non-streaming
        for msg in messages:
            if msg["type"] == "assistant":
                msg_id = msg.get("_raw_data", {}).get("message", {}).get("id")
                if msg_id:
                    streaming_groups[msg_id].append(msg)
                    continue

            # Non-streaming message
            non_streaming.append(msg)

        # Merge streaming groups
        merged_streaming = []
        for _, group in streaming_groups.items():
            if len(group) > 1:
                # Merge the group
                merged = self._merge_message_group(group)
                merged_streaming.append(merged)
            else:
                # Single message, add as-is
                merged_streaming.append(group[0])

        # Combine all messages
        all_merged = non_streaming + merged_streaming

        # Now deduplicate by content key
        seen_content_keys = set()
        deduped = []

        for msg in all_merged:
            # Create deduplication key
            if msg["type"] in ["summary", "compact_summary"]:
                content_key = f"{msg['type']}:{msg.get('timestamp', '')}:{msg.get('content', '')[:200]}"
            else:
                content_preview = msg["content"][:500] if msg["content"] else ""
                timestamp = msg["timestamp"] if msg["timestamp"] else ""
                uuid = msg.get("uuid", "")
                content_key = f"{msg['type']}:{timestamp}:{content_preview}:{uuid}"

            if content_key not in seen_content_keys:
                seen_content_keys.add(content_key)
                deduped.append(msg)

        # Sort by timestamp to maintain order
        deduped.sort(key=lambda x: x["timestamp"] if x["timestamp"] else "", reverse=True)

        return deduped

    def _merge_and_deduplicate_streaming(self, messages: list[dict]) -> list[dict]:
        """Combined streaming merge and deduplication for Phase 2 optimization.

        This combines _merge_streaming_messages functionality with deduplication
        in a single pass for better performance.
        """
        # First do the streaming merge exactly like the original
        # Group by message_id
        message_groups = defaultdict(list)

        for msg in messages:
            if msg["type"] == "assistant":
                # Check for message ID in raw data
                msg_id = None
                if msg.get("_raw_data", {}).get("message", {}).get("id"):
                    msg_id = msg["_raw_data"]["message"]["id"]
                elif msg.get("message_id"):
                    msg_id = msg["message_id"]

                if msg_id:
                    msg["message_id"] = msg_id  # Store for easy access
                    message_groups[msg_id].append(msg)

        # Process groups
        merged = []
        processed_ids = set()

        for msg in messages:
            msg_id = msg.get("message_id")

            # Handle grouped assistant messages
            if msg_id and msg["type"] == "assistant" and msg_id in message_groups and len(message_groups[msg_id]) > 1:
                if msg_id not in processed_ids:
                    # Merge the group
                    group = message_groups[msg_id]
                    merged_msg = self._merge_message_group(group)
                    merged.append(merged_msg)
                    processed_ids.add(msg_id)

            # Add non-grouped messages
            elif not (msg_id and msg["type"] == "assistant" and msg_id in processed_ids):
                merged.append(msg)

        return merged

    def _detect_session_continuations(self, all_files: list[str]) -> dict[str, str]:
        """Detect which sessions are continuations of others with caching."""
        # Check for cache
        cache_file = os.path.join(self.log_directory, ".continuation_cache.json")

        # Check cache validity
        if os.path.exists(cache_file):
            try:
                cache_stat = os.stat(cache_file)
                newest_file = max(os.stat(f).st_mtime for f in all_files) if all_files else 0

                if cache_stat.st_mtime > newest_file:
                    # Cache is newer than all files
                    with open(cache_file, "rb") as f:
                        return orjson.loads(f.read())
            except (OSError, orjson.JSONDecodeError):
                pass  # If cache read fails, continue with detection

        # Build continuations
        continuations = {}

        for i, file_path in enumerate(all_files):
            if i == 0:
                continue  # Skip first file

            # Load first few messages to check for continuation
            first_messages = []
            try:
                with open(file_path, "rb") as f:  # Binary mode for orjson
                    for j, line in enumerate(f):
                        if j >= 5:  # Check first 5 messages
                            break
                        try:
                            msg = orjson.loads(line)
                            first_messages.append(msg)
                        except (orjson.JSONDecodeError, ValueError):
                            continue
            except OSError:
                continue

            for msg in first_messages:
                # Check for compact summary indicating continuation
                if msg.get("isCompactSummary"):
                    session_id = os.path.basename(file_path).replace(".jsonl", "")
                    # Assume it continues from the previous file for now
                    if i > 0:
                        prev_file = all_files[i - 1]
                        prev_session_id = os.path.basename(prev_file).replace(".jsonl", "")
                        continuations[session_id] = prev_session_id
                    break

                # Check for "continue" command
                if msg.get("type") == "user" and msg.get("message"):
                    content = self._extract_message_content(msg)
                    if content.strip().lower() == "continue":
                        session_id = os.path.basename(file_path).replace(".jsonl", "")
                        if i > 0:
                            prev_file = all_files[i - 1]
                            prev_session_id = os.path.basename(prev_file).replace(".jsonl", "")
                            continuations[session_id] = prev_session_id
                        break

        # Save to cache
        try:
            with open(cache_file, "wb") as f:
                f.write(orjson.dumps(continuations))
        except OSError:
            pass  # If cache write fails, continue without caching

        return continuations

    def _extract_message_content(self, msg: dict) -> str:
        """Extract text content from a message."""
        content_parts = []
        if msg.get("message", {}).get("content"):
            content_list = msg["message"]["content"]
            if isinstance(content_list, list):
                for part in content_list:
                    if isinstance(part, dict) and part.get("type") == "text":
                        content_parts.append(part.get("text", ""))
        return " ".join(content_parts)

    def _group_into_interactions(self, messages: list[dict], session_metadata: dict) -> list[Interaction]:
        """Group messages into complete interactions."""
        interactions = []
        current_interaction = None

        for msg in messages:
            msg_type = msg.get("type", "")

            # Check if it's a real user message (not tool result)
            is_tool_result = False

            # Check both raw and processed formats
            if msg_type == "user":
                # Check raw format first
                if msg.get("_raw_data", {}).get("message", {}).get("content"):
                    for content in msg["_raw_data"]["message"]["content"]:
                        if isinstance(content, dict) and content.get("type") == "tool_result":
                            is_tool_result = True
                            break
                # Fallback to processed format
                elif msg.get("has_tool_result"):
                    is_tool_result = True
                # Check content for tool result markers
                elif msg.get("content", "").startswith("[Tool Result:") or msg.get("content", "").startswith(
                    "[Tool Error:"
                ):
                    is_tool_result = True

            if msg_type == "user" and not is_tool_result:
                # Start new interaction
                if current_interaction:
                    interactions.append(current_interaction)
                current_interaction = Interaction(msg)

            elif current_interaction:
                if msg_type == "assistant":
                    current_interaction.add_assistant_message(msg)
                elif msg_type == "user" and is_tool_result:
                    current_interaction.add_tool_result(msg)

        # Don't forget the last interaction
        if current_interaction:
            interactions.append(current_interaction)

        return interactions

    def _handle_split_interactions(self, interactions: list[Interaction], session_metadata: dict):
        """Handle interactions split across file boundaries."""
        # Group interactions by session
        interactions_by_session = defaultdict(list)
        for interaction in interactions:
            interactions_by_session[interaction.session_id].append(interaction)

        # Check for split interactions
        for session_id, session_interactions in interactions_by_session.items():
            if not session_interactions:
                continue

            # Check last interaction of this session
            last_interaction = session_interactions[-1]

            # If it has no assistant response, look in the next session
            if not last_interaction.assistant_messages:
                # Find next session
                metadata = session_metadata.get(session_id, {})
                current_index = metadata.get("index", -1)

                # Look for the next session in order
                for other_session_id, other_metadata in session_metadata.items():
                    if other_metadata.get("index") == current_index + 1:
                        # Check first messages of next session
                        next_interactions = interactions_by_session.get(other_session_id, [])
                        if next_interactions:
                            # Check if first interaction starts with assistant message
                            first_interaction = next_interactions[0]
                            if first_interaction.assistant_messages and not first_interaction.user_message:
                                # Merge the assistant messages into the previous interaction
                                for msg in first_interaction.assistant_messages:
                                    last_interaction.add_assistant_message(msg)
                                for msg in first_interaction.tool_results:
                                    last_interaction.add_tool_result(msg)
                                # Remove the incomplete interaction
                                interactions.remove(first_interaction)
                        break

    def _merge_duplicate_interactions(self, interactions: list[Interaction]) -> list[Interaction]:
        """Merge duplicate interactions, selecting best data."""
        # Group by interaction ID
        interaction_groups = defaultdict(list)
        for interaction in interactions:
            interaction_groups[interaction.interaction_id].append(interaction)

        merged_interactions = []
        for _, duplicates in interaction_groups.items():
            if len(duplicates) == 1:
                merged_interactions.append(duplicates[0])
                continue

            # Sort by completeness score (highest first)
            duplicates.sort(key=lambda x: x.completeness_score(), reverse=True)

            # Start with most complete
            best = duplicates[0]

            # Merge additional data from others
            for other in duplicates[1:]:
                # Merge tools
                best.merge_tools_from(other)

                # Update model if missing
                if best.model == "N/A" and other.model != "N/A":
                    best.model = other.model

                # Merge assistant messages if more complete
                if not best.has_complete_response() and other.has_complete_response():
                    best.assistant_messages = other.assistant_messages

            merged_interactions.append(best)

        return merged_interactions

    def _reconcile_all_tool_counts(self, interactions: list[Interaction]):
        """Reconcile tool counts for all interactions."""
        for interaction in interactions:
            interaction.final_tool_count = self._reconcile_tool_count(interaction)

    def _reconcile_tool_count(self, interaction: Interaction) -> int:
        """Accurately count tools for an interaction."""
        tool_count = 0
        seen_tool_ids = set()

        # Count from tools_used (already extracted)
        for tool in interaction.tools_used:
            tool_id = tool.get("id")
            if tool_id and tool_id not in seen_tool_ids:
                seen_tool_ids.add(tool_id)
                tool_count += 1

        # Verify against tool results
        tool_results_count = 0
        for msg in interaction.tool_results:
            if msg.get("message", {}).get("content"):
                for content in msg["message"]["content"]:
                    if content.get("type") == "tool_result":
                        tool_results_count += 1

        # Handle Task tool special case
        if interaction.has_task_tool:
            # Task tool counts as 1, internal operations not logged
            tool_count = max(tool_count, 1)

        # Reconcile mismatches
        if tool_results_count > tool_count and not interaction.has_task_tool:
            # More results than uses - likely streaming issue
            tool_count = tool_results_count

        # Check for evidence in content if count is 0
        if tool_count == 0:
            tool_count = self._infer_tool_count_from_content(interaction)

        return tool_count

    def _infer_tool_count_from_content(self, interaction: Interaction) -> int:
        """Infer tool usage from assistant message content."""
        tool_patterns = {
            "Read": r"(?:Read|Reading|Examined|Looking at) (?:file|the file|contents of) .+",
            "Edit": r"(?:Edit|Edited|Modified|Updated|Changed) .+",
            "Write": r"(?:Write|Wrote|Created|Creating) .+",
            "Bash": r"(?:Ran|Executed|Running|Executing) (?:command|bash|script)",
            "Grep": r"(?:Searched|Grepped|Found|Searching) .+ (?:in|across)",
            "Task": r"(?:Created task|Task completed|Working on task|Launching)",
        }

        tools_found = set()
        for msg in interaction.assistant_messages:
            content = self._extract_message_content(msg)
            for tool_name, pattern in tool_patterns.items():
                if re.search(pattern, content, re.IGNORECASE):
                    tools_found.add(tool_name)

        # Check for explicit tool result indicators
        for msg in interaction.assistant_messages:
            content = self._extract_message_content(msg)
            if "[Tool Execution Result]" in content or "Used " in content:
                # At least one tool was used
                return max(1, len(tools_found))

        return len(tools_found)

    def _interactions_to_messages(self, interactions: list[Interaction]) -> list[dict]:
        """Convert interactions back to message format for compatibility."""
        messages = []

        for interaction in interactions:
            # Add user message
            user_msg = interaction.user_message.copy()
            # Add tool count info to user message for command analysis
            user_msg["interaction_tool_count"] = interaction.final_tool_count
            user_msg["interaction_model"] = interaction.model
            user_msg["interaction_assistant_steps"] = len(interaction.assistant_messages)
            messages.append(user_msg)

            # Add assistant messages
            messages.extend(interaction.assistant_messages)

            # Add tool results
            messages.extend(interaction.tool_results)

        return messages


# For local testing, use: python -m core.processor /path/to/logs

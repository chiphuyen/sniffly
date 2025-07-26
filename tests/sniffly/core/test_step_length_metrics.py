#!/usr/bin/env python3
"""
Tests for step-length metrics feature.
Step-length measures consecutive tool uses before interruption.
"""

import json
import os
import sys
import unittest
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sniffly.core.processor import ClaudeLogProcessor
from sniffly.core.stats import StatisticsGenerator


class TestStepLengthMetrics(unittest.TestCase):
    """Test suite for step-length metrics calculations"""

    @classmethod
    def setUpClass(cls):
        """Set up test data directory and process logs once"""
        cls.test_data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "mock-data", "-Users-chip-dev-ai-music"
        )

        # Process logs to get messages and statistics
        processor = ClaudeLogProcessor(cls.test_data_dir)
        cls.messages, cls.statistics = processor.process_logs()

    def test_step_length_metrics_exist(self):
        """Test that step-length metrics are included in user_interactions"""
        user_interactions = self.statistics.get("user_interactions", {})

        # Verify new metrics exist
        self.assertIn("average_step_length", user_interactions, "Should have average_step_length metric")
        self.assertIn("max_step_length", user_interactions, "Should have max_step_length metric")
        self.assertIn("min_step_length", user_interactions, "Should have min_step_length metric")
        self.assertIn("step_length_distribution", user_interactions, "Should have step_length_distribution")
        self.assertIn("step_sequences", user_interactions, "Should have detailed step_sequences")

    def test_average_step_length_calculation(self):
        """Test that average step length is calculated correctly"""
        user_interactions = self.statistics.get("user_interactions", {})

        avg_step_length = user_interactions.get("average_step_length", 0)
        self.assertIsInstance(avg_step_length, (int, float), "Average step length should be numeric")
        self.assertGreaterEqual(avg_step_length, 0, "Average step length should be non-negative")

        # Manual verification if we have step sequences
        if "step_sequences" in user_interactions:
            sequences = user_interactions["step_sequences"]
            if sequences:
                manual_avg = sum(seq["length"] for seq in sequences) / len(sequences)
                self.assertAlmostEqual(
                    avg_step_length, manual_avg, 2, "Average calculation should match manual calculation"
                )

    def test_max_min_step_length(self):
        """Test max and min step length calculations"""
        user_interactions = self.statistics.get("user_interactions", {})

        max_length = user_interactions.get("max_step_length", 0)
        min_length = user_interactions.get("min_step_length", 0)

        self.assertGreaterEqual(max_length, 0, "Max step length should be non-negative")
        self.assertGreaterEqual(min_length, 0, "Min step length should be non-negative")
        self.assertGreaterEqual(max_length, min_length, "Max should be >= min step length")

        # If we have sequences, verify against actual data
        if "step_sequences" in user_interactions and user_interactions["step_sequences"]:
            sequences = user_interactions["step_sequences"]
            actual_max = max(seq["length"] for seq in sequences)
            actual_min = min(seq["length"] for seq in sequences)
            self.assertEqual(max_length, actual_max, "Max should match actual max")
            self.assertEqual(min_length, actual_min, "Min should match actual min")

    def test_step_length_distribution(self):
        """Test step length distribution is properly formatted"""
        user_interactions = self.statistics.get("user_interactions", {})
        distribution = user_interactions.get("step_length_distribution", {})

        self.assertIsInstance(distribution, dict, "Distribution should be a dictionary")

        # Keys should be step lengths (as strings), values should be counts
        for length_str, count in distribution.items():
            self.assertTrue(
                length_str.isdigit() or length_str == "10+",
                f"Distribution key '{length_str}' should be numeric or '10+'",
            )
            self.assertIsInstance(count, int, f"Count for length {length_str} should be integer")
            self.assertGreater(count, 0, f"Count for length {length_str} should be positive")

    def test_step_sequences_structure(self):
        """Test that step sequences have the correct structure"""
        user_interactions = self.statistics.get("user_interactions", {})
        sequences = user_interactions.get("step_sequences", [])

        self.assertIsInstance(sequences, list, "Step sequences should be a list")

        for i, seq in enumerate(sequences):
            self.assertIsInstance(seq, dict, f"Sequence {i} should be a dictionary")

            # Required fields
            self.assertIn("length", seq, f"Sequence {i} should have length")
            self.assertIn("tools", seq, f"Sequence {i} should have tools list")
            self.assertIn("interrupted_by", seq, f"Sequence {i} should have interrupted_by")
            self.assertIn("timestamp", seq, f"Sequence {i} should have timestamp")

            # Type checks
            self.assertIsInstance(seq["length"], int, f"Sequence {i} length should be integer")
            self.assertIsInstance(seq["tools"], list, f"Sequence {i} tools should be a list")
            self.assertIn(
                seq["interrupted_by"],
                ["user", "error", "completion", None],
                f"Sequence {i} interrupted_by should be valid type",
            )

            # Length represents number of commands, not individual tools
            # So tools list can be longer than length
            self.assertGreater(len(seq["tools"]), 0, f"Sequence {i} should have at least one tool")

    def test_step_length_by_tool_type(self):
        """Test step length analysis by tool type"""
        user_interactions = self.statistics.get("user_interactions", {})

        # Optional metric: step length by tool type
        if "step_length_by_tool" in user_interactions:
            by_tool = user_interactions["step_length_by_tool"]

            self.assertIsInstance(by_tool, dict, "Step length by tool should be a dictionary")

            for tool, stats in by_tool.items():
                self.assertIsInstance(stats, dict, f"Stats for {tool} should be a dictionary")
                self.assertIn("average_length", stats, f"{tool} should have average_length")
                self.assertIn("max_length", stats, f"{tool} should have max_length")
                self.assertIn("frequency", stats, f"{tool} should have frequency")

    def test_step_length_correlation_with_interruptions(self):
        """Test that step length correlates with interruption data"""
        user_interactions = self.statistics.get("user_interactions", {})

        # Get interruption rate and step length
        interruption_rate = user_interactions.get("interruption_rate", 0)
        avg_step_length = user_interactions.get("average_step_length", 0)

        # Generally, higher interruption rate should mean lower step length
        # This is a soft check - not always true but useful to verify
        if interruption_rate > 50 and avg_step_length > 5:
            self.fail("High interruption rate with high step length seems inconsistent")

    def test_empty_or_no_tools_case(self):
        """Test edge case where no tools are used"""
        # Create a minimal stats generator with no tool usage
        minimal_messages = [
            {
                "type": "user",
                "content": "Hello",
                "timestamp": "2024-01-01T10:00:00Z",
                "session_id": "test-session",
                "error": False,
                "tools": [],
                "tokens": {},
            },
            {
                "type": "assistant",
                "content": "Hi there!",
                "timestamp": "2024-01-01T10:00:01Z",
                "session_id": "test-session",
                "tokens": {"input": 10, "output": 5},
                "model": "claude-3-opus-20240229",
                "error": False,
                "tools": [],
            },
        ]

        # Create minimal running stats required by StatisticsGenerator
        minimal_running_stats = {
            "message_counts": defaultdict(int, {"user": 1, "assistant": 1}),
            "tokens": {"input": 10, "output": 5},
            "tool_usage": defaultdict(int),
            "tool_errors": defaultdict(int),
            "tool_search_usage": defaultdict(int),
            "model_usage": {
                "claude-3-opus-20240229": {
                    "count": 1,
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "cache_creation": 0,
                    "cache_read": 0,
                }
            },
        }

        stats_gen = StatisticsGenerator("test", minimal_running_stats)
        stats = stats_gen.generate_statistics(minimal_messages)

        user_interactions = stats.get("user_interactions", {})

        # Should handle gracefully with zeros
        self.assertEqual(
            user_interactions.get("average_step_length", 0), 0, "Average step length should be 0 with no tools"
        )
        self.assertEqual(user_interactions.get("max_step_length", 0), 0, "Max step length should be 0 with no tools")
        self.assertEqual(
            len(user_interactions.get("step_sequences", [])), 0, "Should have no step sequences with no tools"
        )


if __name__ == "__main__":
    unittest.main()

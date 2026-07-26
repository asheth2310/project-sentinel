"""Sliding window state model for Project Sentinel.

Defines the WindowState dataclass used by the anomaly engine to maintain
per-agent sliding window aggregations (Requirement 7).
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID


@dataclass
class WindowState:
    """Per-agent sliding window aggregation state (Requirement 7).

    Maintains aggregated telemetry metrics over a configurable time window
    (default 60 seconds) for real-time anomaly detection.

    Tracks:
    - Token and cost totals
    - Event count
    - Unique tool calls and prompt hashes
    - Latency statistics (max and average)
    - Token growth rate (tokens per second trend)
    - Consecutive identical tool call count
    - Last tool name for consecutive call tracking
    """

    agent_id: UUID
    window_start: datetime
    window_end: datetime
    total_tokens: int = 0
    total_cost: Decimal = field(default_factory=lambda: Decimal("0"))
    event_count: int = 0
    unique_tool_calls: set[str] = field(default_factory=set)
    unique_prompt_hashes: set[str] = field(default_factory=set)
    max_latency_ms: int = 0
    avg_latency_ms: float = 0.0
    token_growth_rate: float = 0.0
    consecutive_identical_calls: int = 0
    last_tool_name: Optional[str] = None
    token_sum_squares: float = 0.0

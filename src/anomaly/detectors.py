"""Anomaly detection algorithms for Project Sentinel.

Implements detection rules for:
- Infinite loop detection (consecutive identical tool calls)
- Prompt cascade detection (exponential token growth rate)
- Token spike detection (Z-score based statistical deviation)
- Severity classification based on threshold exceedance magnitude

Requirements 4, 5, 6.
"""

import math
from datetime import datetime, timezone
from uuid import UUID

from src.models.anomaly import AnomalyEvent, AnomalyType, Severity
from src.models.telemetry import TelemetryEvent
from src.models.window import WindowState


class AnomalyDetector:
    """Anomaly detection engine that evaluates WindowState against configurable thresholds.

    Runs detection rules for infinite loops, prompt cascades, and token spikes,
    producing AnomalyEvent instances when anomalous behavior is detected.
    """

    def __init__(
        self,
        loop_threshold: int = 10,
        cascade_rate_threshold: float = 1000.0,
        spike_z_threshold: float = 3.0,
        min_events_for_spike: int = 2,
    ) -> None:
        """Initialize the detector with configurable thresholds.

        Args:
            loop_threshold: Number of consecutive identical tool calls to trigger
                INFINITE_LOOP (default 10). Soft warning at 50%.
            cascade_rate_threshold: Token growth rate (tokens/sec) to trigger
                PROMPT_CASCADE (default 1000.0).
            spike_z_threshold: Z-score threshold to trigger TOKEN_SPIKE (default 3.0).
            min_events_for_spike: Minimum prior events required for spike detection
                (default 2).
        """
        if loop_threshold <= 0:
            raise ValueError("loop_threshold must be positive")
        if cascade_rate_threshold <= 0:
            raise ValueError("cascade_rate_threshold must be positive")
        if spike_z_threshold <= 0:
            raise ValueError("spike_z_threshold must be positive")
        if min_events_for_spike < 2:
            raise ValueError("min_events_for_spike must be at least 2")

        self.loop_threshold = loop_threshold
        self.cascade_rate_threshold = cascade_rate_threshold
        self.spike_z_threshold = spike_z_threshold
        self.min_events_for_spike = min_events_for_spike

    def detect_anomalies(
        self, event: TelemetryEvent, window: WindowState
    ) -> list[AnomalyEvent]:
        """Run all detection rules and return any anomalies found.

        Args:
            event: The latest telemetry event being processed.
            window: The current sliding window state for the agent.

        Returns:
            List of AnomalyEvent instances (may be empty if no anomalies detected).
        """
        anomalies: list[AnomalyEvent] = []

        loop_anomaly = self.detect_infinite_loop(window)
        if loop_anomaly is not None:
            anomalies.append(loop_anomaly)

        cascade_anomaly = self.detect_prompt_cascade(window)
        if cascade_anomaly is not None:
            anomalies.append(cascade_anomaly)

        spike_anomaly = self.detect_token_spike(event, window)
        if spike_anomaly is not None:
            anomalies.append(spike_anomaly)

        return anomalies

    def detect_infinite_loop(self, window: WindowState) -> AnomalyEvent | None:
        """Detect when consecutive identical tool calls reach threshold.

        Requirements 4.1-4.5:
        - Track consecutive identical tool calls per agent
        - When count >= loop_threshold, produce INFINITE_LOOP anomaly
        - Soft warning at 50% of threshold (detection resets on different call)

        Args:
            window: The current sliding window state for the agent.

        Returns:
            An AnomalyEvent if the threshold is met, otherwise None.
        """
        count = window.consecutive_identical_calls

        if count < self.loop_threshold:
            return None

        severity = self.classify_severity(
            float(count), float(self.loop_threshold)
        )

        tool_name = window.last_tool_name or "unknown"

        return AnomalyEvent(
            agent_id=window.agent_id,
            org_id=window.agent_id,  # org_id set to agent_id as placeholder
            anomaly_type=AnomalyType.INFINITE_LOOP,
            severity=severity,
            detected_at=datetime.now(timezone.utc),
            window_start=window.window_start,
            window_end=window.window_end,
            metric_value=float(count),
            threshold_value=float(self.loop_threshold),
            description=(
                f"Agent made {count} consecutive identical calls to '{tool_name}' "
                f"(threshold: {self.loop_threshold})"
            ),
            metadata={
                "tool_name": tool_name,
                "consecutive_count": count,
                "threshold": self.loop_threshold,
            },
        )

    def detect_prompt_cascade(self, window: WindowState) -> AnomalyEvent | None:
        """Detect exponential token growth (tokens/sec exceeds threshold).

        Requirements 5.1-5.5:
        - Compute token growth rate within sliding window
        - When rate > cascade_rate_threshold, produce PROMPT_CASCADE anomaly

        Args:
            window: The current sliding window state for the agent.

        Returns:
            An AnomalyEvent if the growth rate exceeds the threshold, otherwise None.
        """
        rate = window.token_growth_rate

        if rate <= self.cascade_rate_threshold:
            return None

        severity = self.classify_severity(rate, self.cascade_rate_threshold)

        return AnomalyEvent(
            agent_id=window.agent_id,
            org_id=window.agent_id,  # org_id set to agent_id as placeholder
            anomaly_type=AnomalyType.PROMPT_CASCADE,
            severity=severity,
            detected_at=datetime.now(timezone.utc),
            window_start=window.window_start,
            window_end=window.window_end,
            metric_value=rate,
            threshold_value=self.cascade_rate_threshold,
            description=(
                f"Token growth rate {rate:.1f} tokens/sec exceeds threshold "
                f"{self.cascade_rate_threshold:.1f} tokens/sec"
            ),
            metadata={
                "token_growth_rate": rate,
                "threshold": self.cascade_rate_threshold,
                "total_tokens": window.total_tokens,
            },
        )

    def detect_token_spike(
        self, event: TelemetryEvent, window: WindowState
    ) -> AnomalyEvent | None:
        """Detect sudden statistical spikes using Z-score.

        Requirements 6.1-6.5:
        - Compute Z-score for event's total tokens relative to window distribution
        - Requires at least min_events_for_spike prior events
        - When Z-score > spike_z_threshold, produce TOKEN_SPIKE anomaly
        - Single-event windows (first event) do not trigger detection

        Args:
            event: The latest telemetry event being processed.
            window: The current sliding window state for the agent.

        Returns:
            An AnomalyEvent if the Z-score exceeds the threshold, otherwise None.
        """
        # Require at least min_events_for_spike events in the window
        if window.event_count < self.min_events_for_spike:
            return None

        event_tokens = float(event.prompt_tokens + event.completion_tokens)

        z_score = self.compute_z_score(event_tokens, window)

        if z_score <= self.spike_z_threshold:
            return None

        severity = self.classify_severity(z_score, self.spike_z_threshold)

        return AnomalyEvent(
            agent_id=window.agent_id,
            org_id=event.org_id,
            anomaly_type=AnomalyType.TOKEN_SPIKE,
            severity=severity,
            detected_at=datetime.now(timezone.utc),
            window_start=window.window_start,
            window_end=window.window_end,
            metric_value=z_score,
            threshold_value=self.spike_z_threshold,
            description=(
                f"Token spike detected: Z-score {z_score:.2f} exceeds threshold "
                f"{self.spike_z_threshold:.2f} "
                f"(event tokens: {int(event_tokens)}, "
                f"window avg: {window.total_tokens / window.event_count:.1f})"
            ),
            metadata={
                "z_score": z_score,
                "event_tokens": int(event_tokens),
                "window_event_count": window.event_count,
                "window_total_tokens": window.total_tokens,
                "threshold": self.spike_z_threshold,
            },
        )

    def classify_severity(self, value: float, threshold: float) -> Severity:
        """Classify severity based on how far value exceeds threshold.

        The ratio of value to threshold determines severity:
        - 1.0x - 1.5x threshold: LOW
        - 1.5x - 2.0x threshold: MEDIUM
        - 2.0x - 3.0x threshold: HIGH
        - 3.0x+ threshold: CRITICAL

        Args:
            value: The measured metric value.
            threshold: The threshold value that was exceeded.

        Returns:
            Severity classification.
        """
        if threshold <= 0:
            return Severity.CRITICAL

        ratio = value / threshold

        if ratio >= 3.0:
            return Severity.CRITICAL
        elif ratio >= 2.0:
            return Severity.HIGH
        elif ratio >= 1.5:
            return Severity.MEDIUM
        else:
            return Severity.LOW

    def compute_z_score(self, value: float, window: WindowState) -> float:
        """Compute Z-score for a value relative to the window's distribution.

        Z-score = |value - mean| / std_dev

        Standard deviation is computed from the window's token_sum_squares:
            variance = (sum_squares / n) - mean^2
            std_dev = sqrt(variance)

        Preconditions:
        - window.event_count >= 2 (need at least 2 data points for std dev)
        - std_dev > 0 (returns 0.0 if all values are identical)

        Args:
            value: The value to compute Z-score for (typically event token count).
            window: The window state containing aggregate statistics.

        Returns:
            Absolute Z-score >= 0. Returns 0.0 if preconditions are not met.
        """
        if window.event_count < 2:
            return 0.0

        n = window.event_count
        mean = window.total_tokens / n

        # Compute population variance from sum of squares
        # variance = E[X^2] - (E[X])^2 = sum_squares/n - mean^2
        variance = (window.token_sum_squares / n) - (mean ** 2)

        # Guard against floating-point errors giving tiny negative variance
        if variance <= 0:
            return 0.0

        std_dev = math.sqrt(variance)

        if std_dev <= 0:
            return 0.0

        z_score = abs(value - mean) / std_dev
        return z_score

"""Tests for anomaly detection algorithms (Tasks 30-33).

Tests cover:
- Infinite loop detection (Task 30)
- Prompt cascade detection (Task 31)
- Token spike detection with Z-score (Task 32)
- Severity classification (Task 33)
"""

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from src.anomaly.detectors import AnomalyDetector
from src.models.anomaly import AnomalyType, Severity
from src.models.telemetry import TelemetryEvent
from src.models.window import WindowState


# --- Fixtures ---


@pytest.fixture
def detector() -> AnomalyDetector:
    """Default detector with standard thresholds."""
    return AnomalyDetector(
        loop_threshold=10,
        cascade_rate_threshold=1000.0,
        spike_z_threshold=3.0,
        min_events_for_spike=2,
    )


@pytest.fixture
def agent_id():
    return uuid4()


@pytest.fixture
def org_id():
    return uuid4()


def make_window(
    agent_id,
    event_count: int = 5,
    total_tokens: int = 500,
    token_sum_squares: float = 50000.0,
    consecutive_identical_calls: int = 0,
    last_tool_name: str | None = None,
    token_growth_rate: float = 0.0,
) -> WindowState:
    """Helper to create a WindowState with specified metrics."""
    now = datetime.now(timezone.utc)
    return WindowState(
        agent_id=agent_id,
        window_start=now - timedelta(seconds=60),
        window_end=now,
        total_tokens=total_tokens,
        total_cost=Decimal("0.05"),
        event_count=event_count,
        token_growth_rate=token_growth_rate,
        consecutive_identical_calls=consecutive_identical_calls,
        last_tool_name=last_tool_name,
        token_sum_squares=token_sum_squares,
    )


def make_event(
    agent_id,
    org_id,
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
) -> TelemetryEvent:
    """Helper to create a TelemetryEvent."""
    return TelemetryEvent(
        timestamp=datetime.now(timezone.utc),
        agent_id=agent_id,
        org_id=org_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_cost=Decimal("0.01"),
        latency_ms=100,
        tool_name="test_tool",
    )


# --- Task 30: Infinite Loop Detection ---


class TestInfiniteLoopDetector:
    """Tests for detect_infinite_loop (Requirement 4)."""

    def test_no_anomaly_below_threshold(self, detector, agent_id):
        """No anomaly when consecutive calls are below threshold."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=9,
            last_tool_name="search",
        )
        result = detector.detect_infinite_loop(window)
        assert result is None

    def test_anomaly_at_threshold(self, detector, agent_id):
        """INFINITE_LOOP anomaly when consecutive calls reach threshold."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=10,
            last_tool_name="search",
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.anomaly_type == AnomalyType.INFINITE_LOOP
        assert result.metric_value == 10.0
        assert result.threshold_value == 10.0

    def test_anomaly_above_threshold(self, detector, agent_id):
        """INFINITE_LOOP anomaly when consecutive calls exceed threshold."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=15,
            last_tool_name="search",
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.anomaly_type == AnomalyType.INFINITE_LOOP
        assert result.metric_value == 15.0

    def test_includes_tool_name_in_metadata(self, detector, agent_id):
        """Anomaly metadata includes the repeated tool name."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=10,
            last_tool_name="code_execute",
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.metadata["tool_name"] == "code_execute"
        assert result.metadata["consecutive_count"] == 10

    def test_no_tool_name_uses_unknown(self, detector, agent_id):
        """When last_tool_name is None, uses 'unknown'."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=10,
            last_tool_name=None,
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.metadata["tool_name"] == "unknown"

    def test_zero_consecutive_calls(self, detector, agent_id):
        """No anomaly with zero consecutive calls."""
        window = make_window(
            agent_id,
            consecutive_identical_calls=0,
        )
        result = detector.detect_infinite_loop(window)
        assert result is None

    def test_custom_threshold(self, agent_id):
        """Detector respects custom loop threshold."""
        detector = AnomalyDetector(loop_threshold=5)
        window = make_window(
            agent_id,
            consecutive_identical_calls=5,
            last_tool_name="api_call",
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.threshold_value == 5.0


# --- Task 31: Prompt Cascade Detection ---


class TestPromptCascadeDetector:
    """Tests for detect_prompt_cascade (Requirement 5)."""

    def test_no_anomaly_below_threshold(self, detector, agent_id):
        """No anomaly when growth rate is below threshold."""
        window = make_window(
            agent_id,
            token_growth_rate=999.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is None

    def test_no_anomaly_at_threshold(self, detector, agent_id):
        """No anomaly when growth rate exactly equals threshold."""
        window = make_window(
            agent_id,
            token_growth_rate=1000.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is None

    def test_anomaly_above_threshold(self, detector, agent_id):
        """PROMPT_CASCADE anomaly when growth rate exceeds threshold."""
        window = make_window(
            agent_id,
            token_growth_rate=1500.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is not None
        assert result.anomaly_type == AnomalyType.PROMPT_CASCADE
        assert result.metric_value == 1500.0
        assert result.threshold_value == 1000.0

    def test_includes_metadata(self, detector, agent_id):
        """Anomaly metadata includes growth rate details."""
        window = make_window(
            agent_id,
            token_growth_rate=2000.0,
            total_tokens=120000,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is not None
        assert result.metadata["token_growth_rate"] == 2000.0
        assert result.metadata["total_tokens"] == 120000

    def test_zero_growth_rate(self, detector, agent_id):
        """No anomaly with zero growth rate."""
        window = make_window(
            agent_id,
            token_growth_rate=0.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is None

    def test_custom_threshold(self, agent_id):
        """Detector respects custom cascade rate threshold."""
        detector = AnomalyDetector(cascade_rate_threshold=500.0)
        window = make_window(
            agent_id,
            token_growth_rate=501.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is not None
        assert result.threshold_value == 500.0


# --- Task 32: Token Spike Detection ---


class TestTokenSpikeDetector:
    """Tests for detect_token_spike with Z-score (Requirement 6)."""

    def test_no_anomaly_with_single_event(self, detector, agent_id, org_id):
        """No spike detection with only 1 event in window (Req 6.5)."""
        window = make_window(agent_id, event_count=1, total_tokens=100, token_sum_squares=10000.0)
        event = make_event(agent_id, org_id, prompt_tokens=500, completion_tokens=500)
        result = detector.detect_token_spike(event, window)
        assert result is None

    def test_no_anomaly_below_z_threshold(self, detector, agent_id, org_id):
        """No anomaly when Z-score is below threshold."""
        # Window: 5 events, total 500 tokens, avg = 100
        # sum_squares for 5 events all at 100: 5 * 10000 = 50000
        # variance = 50000/5 - 100^2 = 10000 - 10000 = 0
        # With zero variance, z-score is 0
        # Let's use a window with some variance instead
        # 5 events: [80, 90, 100, 110, 120] -> sum=500, sum_sq=50600
        # variance = 50600/5 - 100^2 = 10120 - 10000 = 120, std=10.95
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=50600.0,
        )
        # Event with 120 tokens: z = |120 - 100| / 10.95 ≈ 1.83
        event = make_event(agent_id, org_id, prompt_tokens=70, completion_tokens=50)
        result = detector.detect_token_spike(event, window)
        assert result is None

    def test_anomaly_above_z_threshold(self, detector, agent_id, org_id):
        """TOKEN_SPIKE anomaly when Z-score exceeds threshold."""
        # Window: 5 events, total 500 tokens, avg = 100
        # sum_squares: [80,90,100,110,120] -> 6400+8100+10000+12100+14400=51000
        # variance = 51000/5 - 100^2 = 10200 - 10000 = 200, std = 14.14
        # Event with 200 tokens: z = |200 - 100| / 14.14 ≈ 7.07 > 3.0
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=51000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=150, completion_tokens=50)
        result = detector.detect_token_spike(event, window)
        assert result is not None
        assert result.anomaly_type == AnomalyType.TOKEN_SPIKE
        assert result.metric_value > 3.0

    def test_requires_min_events(self, detector, agent_id, org_id):
        """No detection with fewer than min_events_for_spike (Req 6.3)."""
        window = make_window(
            agent_id,
            event_count=1,
            total_tokens=100,
            token_sum_squares=10000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=1000, completion_tokens=0)
        result = detector.detect_token_spike(event, window)
        assert result is None

    def test_exactly_min_events_works(self, detector, agent_id, org_id):
        """Detection works with exactly min_events_for_spike events."""
        # 2 events: [50, 150] -> total=200, avg=100
        # sum_sq = 2500 + 22500 = 25000
        # variance = 25000/2 - 100^2 = 12500 - 10000 = 2500, std = 50
        # Event 500 tokens: z = |500 - 100| / 50 = 8.0 > 3.0
        window = make_window(
            agent_id,
            event_count=2,
            total_tokens=200,
            token_sum_squares=25000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=300, completion_tokens=200)
        result = detector.detect_token_spike(event, window)
        assert result is not None
        assert result.anomaly_type == AnomalyType.TOKEN_SPIKE

    def test_zero_variance_no_anomaly(self, detector, agent_id, org_id):
        """No anomaly when all events are identical (zero std dev)."""
        # 5 events all at 100: sum=500, sum_sq=50000
        # variance = 50000/5 - 100^2 = 0
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=50000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=200, completion_tokens=0)
        result = detector.detect_token_spike(event, window)
        assert result is None

    def test_spike_metadata(self, detector, agent_id, org_id):
        """Token spike anomaly includes correct metadata."""
        # variance = 51000/5 - 100^2 = 200, std = 14.14
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=51000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=150, completion_tokens=50)
        result = detector.detect_token_spike(event, window)
        assert result is not None
        assert result.metadata["event_tokens"] == 200
        assert result.metadata["window_event_count"] == 5
        assert result.metadata["window_total_tokens"] == 500

    def test_custom_z_threshold(self, agent_id, org_id):
        """Detector respects custom Z-score threshold."""
        detector = AnomalyDetector(spike_z_threshold=1.0)
        # variance = 51000/5 - 100^2 = 200, std = 14.14
        # event 130 tokens: z = |130 - 100| / 14.14 ≈ 2.12 > 1.0
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=51000.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=80, completion_tokens=50)
        result = detector.detect_token_spike(event, window)
        assert result is not None


# --- Task 33: Severity Classification ---


class TestSeverityClassification:
    """Tests for classify_severity (Requirements 4, 5, 6)."""

    def test_low_severity_at_threshold(self, detector):
        """1.0x threshold yields LOW severity."""
        severity = detector.classify_severity(10.0, 10.0)
        assert severity == Severity.LOW

    def test_low_severity_below_1_5x(self, detector):
        """1.4x threshold yields LOW severity."""
        severity = detector.classify_severity(14.0, 10.0)
        assert severity == Severity.LOW

    def test_medium_severity_at_1_5x(self, detector):
        """1.5x threshold yields MEDIUM severity."""
        severity = detector.classify_severity(15.0, 10.0)
        assert severity == Severity.MEDIUM

    def test_medium_severity_below_2x(self, detector):
        """1.9x threshold yields MEDIUM severity."""
        severity = detector.classify_severity(19.0, 10.0)
        assert severity == Severity.MEDIUM

    def test_high_severity_at_2x(self, detector):
        """2.0x threshold yields HIGH severity."""
        severity = detector.classify_severity(20.0, 10.0)
        assert severity == Severity.HIGH

    def test_high_severity_below_3x(self, detector):
        """2.9x threshold yields HIGH severity."""
        severity = detector.classify_severity(29.0, 10.0)
        assert severity == Severity.HIGH

    def test_critical_severity_at_3x(self, detector):
        """3.0x threshold yields CRITICAL severity."""
        severity = detector.classify_severity(30.0, 10.0)
        assert severity == Severity.CRITICAL

    def test_critical_severity_above_3x(self, detector):
        """5.0x threshold yields CRITICAL severity."""
        severity = detector.classify_severity(50.0, 10.0)
        assert severity == Severity.CRITICAL

    def test_zero_threshold_returns_critical(self, detector):
        """Zero threshold always returns CRITICAL."""
        severity = detector.classify_severity(1.0, 0.0)
        assert severity == Severity.CRITICAL

    def test_negative_threshold_returns_critical(self, detector):
        """Negative threshold always returns CRITICAL."""
        severity = detector.classify_severity(1.0, -5.0)
        assert severity == Severity.CRITICAL

    def test_severity_integrated_with_loop(self, agent_id):
        """Loop detector uses classify_severity for anomaly severity."""
        detector = AnomalyDetector(loop_threshold=10)
        # 20 consecutive calls = 2.0x threshold -> HIGH
        window = make_window(
            agent_id,
            consecutive_identical_calls=20,
            last_tool_name="api_call",
        )
        result = detector.detect_infinite_loop(window)
        assert result is not None
        assert result.severity == Severity.HIGH

    def test_severity_integrated_with_cascade(self, agent_id):
        """Cascade detector uses classify_severity for anomaly severity."""
        detector = AnomalyDetector(cascade_rate_threshold=1000.0)
        # 3500 tokens/sec = 3.5x threshold -> CRITICAL
        window = make_window(
            agent_id,
            token_growth_rate=3500.0,
        )
        result = detector.detect_prompt_cascade(window)
        assert result is not None
        assert result.severity == Severity.CRITICAL


# --- Z-score Computation ---


class TestComputeZScore:
    """Tests for compute_z_score method."""

    def test_returns_zero_with_single_event(self, detector, agent_id):
        """Z-score is 0 with fewer than 2 events."""
        window = make_window(agent_id, event_count=1, total_tokens=100, token_sum_squares=10000.0)
        assert detector.compute_z_score(200.0, window) == 0.0

    def test_returns_zero_with_zero_variance(self, detector, agent_id):
        """Z-score is 0 when all events are identical (zero variance)."""
        # 3 events all at 100: sum=300, sum_sq=30000
        window = make_window(agent_id, event_count=3, total_tokens=300, token_sum_squares=30000.0)
        assert detector.compute_z_score(200.0, window) == 0.0

    def test_correct_z_score_computation(self, detector, agent_id):
        """Z-score is computed correctly with known values."""
        # 4 events: [80, 100, 100, 120] -> sum=400, avg=100
        # sum_sq = 6400 + 10000 + 10000 + 14400 = 40800
        # variance = 40800/4 - 100^2 = 10200 - 10000 = 200
        # std_dev = sqrt(200) ≈ 14.14
        # z_score for 150 = |150 - 100| / 14.14 ≈ 3.54
        window = make_window(
            agent_id,
            event_count=4,
            total_tokens=400,
            token_sum_squares=40800.0,
        )
        z = detector.compute_z_score(150.0, window)
        expected = abs(150.0 - 100.0) / math.sqrt(200.0)
        assert abs(z - expected) < 0.01

    def test_z_score_is_non_negative(self, detector, agent_id):
        """Z-score is always non-negative (absolute value)."""
        # Value below mean should still give positive z-score
        window = make_window(
            agent_id,
            event_count=4,
            total_tokens=400,
            token_sum_squares=40800.0,
        )
        z = detector.compute_z_score(50.0, window)
        assert z >= 0.0


# --- detect_anomalies Integration ---


class TestDetectAnomalies:
    """Tests for the top-level detect_anomalies method."""

    def test_no_anomalies_normal_window(self, detector, agent_id, org_id):
        """Returns empty list when all metrics are normal."""
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=500,
            token_sum_squares=50000.0,
            consecutive_identical_calls=3,
            token_growth_rate=100.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=50, completion_tokens=50)
        results = detector.detect_anomalies(event, window)
        assert results == []

    def test_multiple_anomalies_detected(self, agent_id, org_id):
        """Can detect multiple anomalies simultaneously."""
        detector = AnomalyDetector(
            loop_threshold=5,
            cascade_rate_threshold=100.0,
            spike_z_threshold=2.0,
        )
        # Window with loop AND cascade conditions
        # Also make token spike: events [50,50,50,50,50] -> sum=250, avg=50
        # sum_sq = 5*2500 = 12500, var=0, so no spike from zero variance
        # Use different sum_sq for spike: [40,45,50,55,60] sum=250
        # sum_sq = 1600+2025+2500+3025+3600=12750, var=12750/5-2500=50, std=7.07
        # event 100 tokens: z = |100-50|/7.07 ≈ 7.07 > 2.0
        window = make_window(
            agent_id,
            event_count=5,
            total_tokens=250,
            token_sum_squares=12750.0,
            consecutive_identical_calls=5,
            last_tool_name="repeated_tool",
            token_growth_rate=200.0,
        )
        event = make_event(agent_id, org_id, prompt_tokens=60, completion_tokens=40)
        results = detector.detect_anomalies(event, window)
        anomaly_types = {r.anomaly_type for r in results}
        assert AnomalyType.INFINITE_LOOP in anomaly_types
        assert AnomalyType.PROMPT_CASCADE in anomaly_types
        assert AnomalyType.TOKEN_SPIKE in anomaly_types


# --- Constructor Validation ---


class TestDetectorInit:
    """Tests for AnomalyDetector initialization."""

    def test_invalid_loop_threshold(self):
        with pytest.raises(ValueError, match="loop_threshold must be positive"):
            AnomalyDetector(loop_threshold=0)

    def test_invalid_cascade_threshold(self):
        with pytest.raises(ValueError, match="cascade_rate_threshold must be positive"):
            AnomalyDetector(cascade_rate_threshold=-1.0)

    def test_invalid_spike_threshold(self):
        with pytest.raises(ValueError, match="spike_z_threshold must be positive"):
            AnomalyDetector(spike_z_threshold=0.0)

    def test_invalid_min_events(self):
        with pytest.raises(ValueError, match="min_events_for_spike must be at least 2"):
            AnomalyDetector(min_events_for_spike=1)

# Project Sentinel

Enterprise-grade observability and governance platform for multi-agent AI deployments.

## Features

- Real-time telemetry ingestion from AI agent SDKs
- Anomaly detection (token spikes, infinite loops, prompt cascades)
- Automated circuit breakers to prevent runaway costs
- Configurable governance policies with soft/hard thresholds
- Multi-channel notifications (Slack, PagerDuty)

## Development

```bash
pip install -e ".[dev]"
pytest
```

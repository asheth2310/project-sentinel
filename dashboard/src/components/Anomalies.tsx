import { Zap, Repeat, TrendingUp, Clock, ChevronDown } from 'lucide-react'

const anomalyData = [
  {
    id: 'anom-001',
    type: 'TOKEN_SPIKE',
    severity: 'critical',
    agent: 'agent-research-07',
    org: 'Acme Corp',
    metricValue: 8.4,
    threshold: 3.0,
    description: 'Token usage 8.4 standard deviations above mean. Immediate cost impact detected.',
    detectedAt: '2024-07-26T09:45:00Z',
    windowStart: '2024-07-26T09:44:00Z',
    windowEnd: '2024-07-26T09:45:00Z',
    action: 'Kill-switch activated',
  },
  {
    id: 'anom-002',
    type: 'INFINITE_LOOP',
    severity: 'critical',
    agent: 'agent-support-12',
    org: 'Acme Corp',
    metricValue: 47,
    threshold: 10,
    description: 'Agent called web_search 47 consecutive times without variation.',
    detectedAt: '2024-07-26T09:28:00Z',
    windowStart: '2024-07-26T09:27:00Z',
    windowEnd: '2024-07-26T09:28:00Z',
    action: 'Kill-switch activated',
  },
  {
    id: 'anom-003',
    type: 'PROMPT_CASCADE',
    severity: 'high',
    agent: 'agent-code-03',
    org: 'TechStart',
    metricValue: 2450,
    threshold: 1000,
    description: 'Token growth rate 2,450 tokens/sec, exceeding threshold by 2.45x.',
    detectedAt: '2024-07-26T08:32:00Z',
    windowStart: '2024-07-26T08:31:00Z',
    windowEnd: '2024-07-26T08:32:00Z',
    action: 'Warning sent to Slack',
  },
  {
    id: 'anom-004',
    type: 'TOKEN_SPIKE',
    severity: 'medium',
    agent: 'agent-data-15',
    org: 'DataFlow',
    metricValue: 3.8,
    threshold: 3.0,
    description: 'Moderate token spike detected, Z-score 3.8.',
    detectedAt: '2024-07-26T06:15:00Z',
    windowStart: '2024-07-26T06:14:00Z',
    windowEnd: '2024-07-26T06:15:00Z',
    action: 'Warning sent to Slack',
  },
  {
    id: 'anom-005',
    type: 'INFINITE_LOOP',
    severity: 'low',
    agent: 'agent-chat-22',
    org: 'Acme Corp',
    metricValue: 6,
    threshold: 10,
    description: '6 consecutive identical calls. Approaching threshold (soft warning).',
    detectedAt: '2024-07-26T04:50:00Z',
    windowStart: '2024-07-26T04:49:00Z',
    windowEnd: '2024-07-26T04:50:00Z',
    action: 'No action (below threshold)',
  },
]

const typeIcons: Record<string, React.ReactNode> = {
  TOKEN_SPIKE: <Zap size={16} className="text-yellow-400" />,
  INFINITE_LOOP: <Repeat size={16} className="text-red-400" />,
  PROMPT_CASCADE: <TrendingUp size={16} className="text-orange-400" />,
}

const typeLabels: Record<string, string> = {
  TOKEN_SPIKE: 'Token Spike',
  INFINITE_LOOP: 'Infinite Loop',
  PROMPT_CASCADE: 'Prompt Cascade',
}

const severityBadge: Record<string, string> = {
  critical: 'badge-critical',
  high: 'badge-warning',
  medium: 'badge-info',
  low: 'text-gray-500 bg-gray-800/50 border-gray-700 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border',
}

export default function Anomalies() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Anomalies</h2>
          <p className="text-gray-500 mt-1">Detected deviations across your agent fleet</p>
        </div>
        <div className="flex gap-3">
          <button className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-400 hover:text-gray-200">
            Severity <ChevronDown size={14} />
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-400 hover:text-gray-200">
            Type <ChevronDown size={14} />
          </button>
        </div>
      </div>

      {/* Anomaly Cards */}
      <div className="space-y-4">
        {anomalyData.map((anomaly) => (
          <div key={anomaly.id} className="card hover:border-gray-700 transition-colors">
            <div className="flex items-start justify-between">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 bg-gray-800 rounded-lg flex items-center justify-center mt-0.5">
                  {typeIcons[anomaly.type]}
                </div>
                <div>
                  <div className="flex items-center gap-3">
                    <h4 className="text-sm font-semibold text-white">{typeLabels[anomaly.type]}</h4>
                    <span className={severityBadge[anomaly.severity]}>{anomaly.severity}</span>
                  </div>
                  <p className="text-sm text-gray-400 mt-1">{anomaly.description}</p>
                  <div className="flex items-center gap-4 mt-3">
                    <span className="text-xs text-gray-500">
                      Agent: <span className="text-gray-300">{anomaly.agent}</span>
                    </span>
                    <span className="text-xs text-gray-500">
                      Metric: <span className="text-gray-300">{anomaly.metricValue}</span> / {anomaly.threshold}
                    </span>
                    <span className="text-xs text-gray-500 flex items-center gap-1">
                      <Clock size={12} />
                      {new Date(anomaly.detectedAt).toLocaleTimeString()}
                    </span>
                  </div>
                </div>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-500">{anomaly.action}</p>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

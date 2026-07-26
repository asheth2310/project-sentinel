import { AlertTriangle, Repeat, TrendingUp, Zap } from 'lucide-react'

const anomalies = [
  {
    id: 1,
    type: 'TOKEN_SPIKE',
    severity: 'critical',
    agent: 'agent-research-07',
    message: 'Z-score 8.4 (threshold: 3.0)',
    time: '2 min ago',
  },
  {
    id: 2,
    type: 'INFINITE_LOOP',
    severity: 'critical',
    agent: 'agent-support-12',
    message: '47 consecutive web_search calls',
    time: '18 min ago',
  },
  {
    id: 3,
    type: 'PROMPT_CASCADE',
    severity: 'high',
    agent: 'agent-code-03',
    message: 'Growth rate 2,450 tok/s',
    time: '1h ago',
  },
  {
    id: 4,
    type: 'TOKEN_SPIKE',
    severity: 'medium',
    agent: 'agent-data-15',
    message: 'Z-score 3.8 (threshold: 3.0)',
    time: '3h ago',
  },
  {
    id: 5,
    type: 'INFINITE_LOOP',
    severity: 'low',
    agent: 'agent-chat-22',
    message: '6 consecutive calls (warning)',
    time: '5h ago',
  },
]

const typeIcons: Record<string, React.ReactNode> = {
  TOKEN_SPIKE: <Zap size={14} />,
  INFINITE_LOOP: <Repeat size={14} />,
  PROMPT_CASCADE: <TrendingUp size={14} />,
}

const severityStyles: Record<string, string> = {
  critical: 'badge-critical',
  high: 'badge-warning',
  medium: 'badge-info',
  low: 'text-gray-500 bg-gray-800/50 border-gray-700 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border',
}

export default function AnomalyFeed() {
  return (
    <div className="space-y-3">
      {anomalies.map((anomaly) => (
        <div
          key={anomaly.id}
          className="p-3 bg-gray-800/40 border border-gray-800 rounded-lg hover:border-gray-700 transition-colors"
        >
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className="text-gray-400">{typeIcons[anomaly.type]}</span>
              <span className={severityStyles[anomaly.severity]}>
                {anomaly.severity}
              </span>
            </div>
            <span className="text-xs text-gray-600">{anomaly.time}</span>
          </div>
          <p className="text-sm font-medium text-gray-200 mt-2">{anomaly.agent}</p>
          <p className="text-xs text-gray-500 mt-0.5">{anomaly.message}</p>
        </div>
      ))}
    </div>
  )
}

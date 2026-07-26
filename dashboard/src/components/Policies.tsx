import { Plus, Edit2, Trash2, Shield, Bell } from 'lucide-react'

const policies = [
  {
    id: 'pol-001',
    org: 'Acme Corp',
    autoKill: true,
    thresholds: [
      { metric: 'total_tokens', softLimit: 800000, hardLimit: 1000000, window: '1h', cooldown: '5m' },
      { metric: 'total_cost', softLimit: 8.0, hardLimit: 10.0, window: '1h', cooldown: '5m' },
      { metric: 'consecutive_identical_calls', softLimit: 5, hardLimit: 10, window: '60s', cooldown: '60s' },
    ],
    channels: [
      { type: 'slack', target: '#ai-alerts' },
      { type: 'pagerduty', target: 'AI Ops Team' },
    ],
    updatedAt: '2024-07-25T14:30:00Z',
  },
  {
    id: 'pol-002',
    org: 'TechStart',
    autoKill: true,
    thresholds: [
      { metric: 'total_tokens', softLimit: 500000, hardLimit: 700000, window: '1h', cooldown: '5m' },
      { metric: 'total_cost', softLimit: 5.0, hardLimit: 7.0, window: '1h', cooldown: '10m' },
    ],
    channels: [
      { type: 'slack', target: '#engineering' },
    ],
    updatedAt: '2024-07-20T09:15:00Z',
  },
  {
    id: 'pol-003',
    org: 'DataFlow',
    autoKill: false,
    thresholds: [
      { metric: 'total_tokens', softLimit: 2000000, hardLimit: 3000000, window: '1h', cooldown: '10m' },
      { metric: 'consecutive_identical_calls', softLimit: 8, hardLimit: 15, window: '60s', cooldown: '2m' },
    ],
    channels: [
      { type: 'slack', target: '#data-ops' },
      { type: 'pagerduty', target: 'Data Team On-Call' },
    ],
    updatedAt: '2024-07-18T11:00:00Z',
  },
]

const metricLabels: Record<string, string> = {
  total_tokens: 'Total Tokens',
  total_cost: 'Total Cost ($)',
  consecutive_identical_calls: 'Consecutive Calls',
  latency_p99: 'Latency p99 (ms)',
}

export default function Policies() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Governance Policies</h2>
          <p className="text-gray-500 mt-1">Configure thresholds and alerting per organization</p>
        </div>
        <button className="flex items-center gap-2 px-4 py-2 bg-sentinel-600 hover:bg-sentinel-700 text-white rounded-lg text-sm font-medium transition-colors">
          <Plus size={16} />
          New Policy
        </button>
      </div>

      {/* Policy Cards */}
      <div className="space-y-6">
        {policies.map((policy) => (
          <div key={policy.id} className="card">
            {/* Header */}
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-sentinel-900/40 rounded-lg flex items-center justify-center">
                  <Shield size={18} className="text-sentinel-400" />
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-white">{policy.org}</h3>
                  <p className="text-xs text-gray-500">Last updated: {new Date(policy.updatedAt).toLocaleDateString()}</p>
                </div>
              </div>
              <div className="flex items-center gap-3">
                {policy.autoKill ? (
                  <span className="badge-critical">Auto-Kill ON</span>
                ) : (
                  <span className="badge-warning">Auto-Kill OFF</span>
                )}
                <button className="p-2 text-gray-500 hover:text-gray-300 rounded-lg hover:bg-gray-800">
                  <Edit2 size={16} />
                </button>
                <button className="p-2 text-gray-500 hover:text-red-400 rounded-lg hover:bg-gray-800">
                  <Trash2 size={16} />
                </button>
              </div>
            </div>

            {/* Thresholds Table */}
            <div className="mb-6">
              <h4 className="text-sm font-medium text-gray-400 mb-3">Thresholds</h4>
              <div className="bg-gray-800/40 rounded-lg overflow-hidden">
                <table className="w-full">
                  <thead>
                    <tr className="text-xs text-gray-500 border-b border-gray-700/50">
                      <th className="text-left px-4 py-2.5 font-medium">Metric</th>
                      <th className="text-right px-4 py-2.5 font-medium">Soft Limit (Warning)</th>
                      <th className="text-right px-4 py-2.5 font-medium">Hard Limit (Kill)</th>
                      <th className="text-right px-4 py-2.5 font-medium">Window</th>
                      <th className="text-right px-4 py-2.5 font-medium">Cooldown</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-700/30">
                    {policy.thresholds.map((t, i) => (
                      <tr key={i} className="text-sm">
                        <td className="px-4 py-2.5 text-gray-300">{metricLabels[t.metric] || t.metric}</td>
                        <td className="px-4 py-2.5 text-right text-amber-400">{t.softLimit.toLocaleString()}</td>
                        <td className="px-4 py-2.5 text-right text-red-400">{t.hardLimit.toLocaleString()}</td>
                        <td className="px-4 py-2.5 text-right text-gray-500">{t.window}</td>
                        <td className="px-4 py-2.5 text-right text-gray-500">{t.cooldown}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Notification Channels */}
            <div>
              <h4 className="text-sm font-medium text-gray-400 mb-3">Notification Channels</h4>
              <div className="flex gap-3">
                {policy.channels.map((ch, i) => (
                  <div key={i} className="flex items-center gap-2 px-3 py-2 bg-gray-800/40 rounded-lg border border-gray-700/50">
                    <Bell size={14} className="text-gray-500" />
                    <span className="text-sm text-gray-300">{ch.type === 'slack' ? '🔔' : '🚨'} {ch.target}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

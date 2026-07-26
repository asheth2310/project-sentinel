import { Search, Filter, MoreVertical } from 'lucide-react'

const agents = [
  { id: 'agent-research-07', org: 'Acme Corp', status: 'killed', tokens24h: 892400, cost24h: 12.84, anomalies: 3, lastSeen: '2 min ago' },
  { id: 'agent-support-12', org: 'Acme Corp', status: 'killed', tokens24h: 445200, cost24h: 6.42, anomalies: 1, lastSeen: '18 min ago' },
  { id: 'agent-code-03', org: 'TechStart', status: 'active', tokens24h: 1250000, cost24h: 18.75, anomalies: 1, lastSeen: '10s ago' },
  { id: 'agent-data-15', org: 'DataFlow', status: 'active', tokens24h: 780000, cost24h: 11.20, anomalies: 1, lastSeen: '30s ago' },
  { id: 'agent-chat-22', org: 'Acme Corp', status: 'active', tokens24h: 320000, cost24h: 4.80, anomalies: 0, lastSeen: '5s ago' },
  { id: 'agent-writer-01', org: 'ContentAI', status: 'active', tokens24h: 560000, cost24h: 8.40, anomalies: 0, lastSeen: '1s ago' },
  { id: 'agent-analyst-09', org: 'DataFlow', status: 'active', tokens24h: 430000, cost24h: 6.45, anomalies: 0, lastSeen: '15s ago' },
  { id: 'agent-planner-04', org: 'TechStart', status: 'active', tokens24h: 210000, cost24h: 3.15, anomalies: 0, lastSeen: '2s ago' },
]

export default function Agents() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Agents</h2>
          <p className="text-gray-500 mt-1">Monitor and manage your AI agent fleet</p>
        </div>
        <div className="flex gap-3">
          <div className="relative">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
            <input
              type="text"
              placeholder="Search agents..."
              className="pl-9 pr-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-200 placeholder-gray-600 focus:outline-none focus:border-sentinel-600 w-64"
            />
          </div>
          <button className="flex items-center gap-2 px-4 py-2 bg-gray-900 border border-gray-800 rounded-lg text-sm text-gray-400 hover:text-gray-200">
            <Filter size={16} />
            Filter
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="card p-0 overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="text-left text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Agent</th>
              <th className="text-left text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Organization</th>
              <th className="text-left text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Status</th>
              <th className="text-right text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Tokens (24h)</th>
              <th className="text-right text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Cost (24h)</th>
              <th className="text-center text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Anomalies</th>
              <th className="text-right text-xs font-medium text-gray-500 uppercase tracking-wider px-6 py-4">Last Seen</th>
              <th className="px-6 py-4"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/50">
            {agents.map((agent) => (
              <tr key={agent.id} className="hover:bg-gray-800/30 transition-colors">
                <td className="px-6 py-4">
                  <span className="text-sm font-medium text-white">{agent.id}</span>
                </td>
                <td className="px-6 py-4">
                  <span className="text-sm text-gray-400">{agent.org}</span>
                </td>
                <td className="px-6 py-4">
                  {agent.status === 'active' ? (
                    <span className="badge-ok">● Active</span>
                  ) : (
                    <span className="badge-critical">⊘ Killed</span>
                  )}
                </td>
                <td className="px-6 py-4 text-right">
                  <span className="text-sm text-gray-300">{(agent.tokens24h / 1000).toFixed(0)}k</span>
                </td>
                <td className="px-6 py-4 text-right">
                  <span className="text-sm text-gray-300">${agent.cost24h.toFixed(2)}</span>
                </td>
                <td className="px-6 py-4 text-center">
                  {agent.anomalies > 0 ? (
                    <span className="badge-warning">{agent.anomalies}</span>
                  ) : (
                    <span className="text-sm text-gray-600">—</span>
                  )}
                </td>
                <td className="px-6 py-4 text-right">
                  <span className="text-xs text-gray-500">{agent.lastSeen}</span>
                </td>
                <td className="px-6 py-4">
                  <button className="text-gray-600 hover:text-gray-400">
                    <MoreVertical size={16} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

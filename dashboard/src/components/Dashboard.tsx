import { Activity, DollarSign, Zap, ShieldAlert, TrendingUp, Clock } from 'lucide-react'
import TokenChart from './TokenChart'
import AnomalyFeed from './AnomalyFeed'

export default function Dashboard() {
  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-white">Dashboard</h2>
        <p className="text-gray-500 mt-1">Real-time observability across your AI agent fleet</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          icon={<Bot size={20} />}
          label="Active Agents"
          value="47"
          change="+3 today"
          trend="up"
        />
        <StatCard
          icon={<Activity size={20} />}
          label="Events / min"
          value="12,847"
          change="+8.2%"
          trend="up"
        />
        <StatCard
          icon={<DollarSign size={20} />}
          label="Cost (24h)"
          value="$342.18"
          change="-2.1%"
          trend="down"
        />
        <StatCard
          icon={<ShieldAlert size={20} />}
          label="Anomalies (24h)"
          value="6"
          change="2 critical"
          trend="alert"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-lg font-semibold text-white">Token Usage</h3>
              <p className="text-sm text-gray-500">Last 24 hours across all agents</p>
            </div>
            <div className="flex gap-2">
              <button className="px-3 py-1 text-xs bg-sentinel-600/20 text-sentinel-400 rounded-md border border-sentinel-700/50">24h</button>
              <button className="px-3 py-1 text-xs text-gray-500 hover:text-gray-300 rounded-md">7d</button>
              <button className="px-3 py-1 text-xs text-gray-500 hover:text-gray-300 rounded-md">30d</button>
            </div>
          </div>
          <TokenChart />
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold text-white mb-4">Live Anomalies</h3>
          <AnomalyFeed />
        </div>
      </div>

      {/* Active Circuit Breakers */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white">Active Circuit Breakers</h3>
          <span className="badge-critical">2 Active</span>
        </div>
        <div className="space-y-3">
          <CircuitBreakerRow
            agentName="agent-research-07"
            reason="Token spike: 15.2x normal usage"
            activatedAt="2 min ago"
            activatedBy="system"
          />
          <CircuitBreakerRow
            agentName="agent-support-12"
            reason="Infinite loop: web_search called 47 times"
            activatedAt="18 min ago"
            activatedBy="system"
          />
        </div>
      </div>
    </div>
  )
}

function Bot({ size }: { size: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 8V4H8"/><rect width="16" height="12" x="4" y="8" rx="2"/><path d="M2 14h2"/><path d="M20 14h2"/><path d="M15 13v2"/><path d="M9 13v2"/>
    </svg>
  )
}

function StatCard({ icon, label, value, change, trend }: {
  icon: React.ReactNode
  label: string
  value: string
  change: string
  trend: 'up' | 'down' | 'alert'
}) {
  const trendColor = trend === 'up' ? 'text-emerald-400' : trend === 'down' ? 'text-emerald-400' : 'text-red-400'
  
  return (
    <div className="card">
      <div className="flex items-center justify-between mb-3">
        <span className="text-gray-500">{icon}</span>
        <span className={`text-xs font-medium ${trendColor}`}>{change}</span>
      </div>
      <p className="card-header">{label}</p>
      <p className="stat-value">{value}</p>
    </div>
  )
}

function CircuitBreakerRow({ agentName, reason, activatedAt, activatedBy }: {
  agentName: string
  reason: string
  activatedAt: string
  activatedBy: string
}) {
  return (
    <div className="flex items-center justify-between p-4 bg-red-950/20 border border-red-900/30 rounded-lg">
      <div className="flex items-center gap-4">
        <div className="w-10 h-10 bg-red-900/40 rounded-full flex items-center justify-center">
          <ShieldAlert size={18} className="text-red-400" />
        </div>
        <div>
          <p className="text-sm font-medium text-white">{agentName}</p>
          <p className="text-xs text-gray-500">{reason}</p>
        </div>
      </div>
      <div className="flex items-center gap-4">
        <div className="text-right">
          <p className="text-xs text-gray-500">{activatedAt}</p>
          <p className="text-xs text-gray-600">by {activatedBy}</p>
        </div>
        <button className="px-3 py-1.5 text-xs font-medium bg-gray-800 hover:bg-gray-700 text-gray-300 rounded-md border border-gray-700 transition-colors">
          Deactivate
        </button>
      </div>
    </div>
  )
}

import { Shield, LayoutDashboard, Bot, AlertTriangle, ScrollText } from 'lucide-react'

type Page = 'dashboard' | 'agents' | 'anomalies' | 'policies'

interface SidebarProps {
  activePage: Page
  onNavigate: (page: Page) => void
}

const navItems: { id: Page; label: string; icon: React.ReactNode }[] = [
  { id: 'dashboard', label: 'Overview', icon: <LayoutDashboard size={20} /> },
  { id: 'agents', label: 'Agents', icon: <Bot size={20} /> },
  { id: 'anomalies', label: 'Anomalies', icon: <AlertTriangle size={20} /> },
  { id: 'policies', label: 'Policies', icon: <ScrollText size={20} /> },
]

export default function Sidebar({ activePage, onNavigate }: SidebarProps) {
  return (
    <aside className="w-64 bg-gray-900 border-r border-gray-800 flex flex-col">
      {/* Logo */}
      <div className="p-6 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-gradient-to-br from-sentinel-500 to-purple-600 rounded-lg flex items-center justify-center shadow-md shadow-sentinel-600/20">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <circle cx="12" cy="12" r="9" stroke="white" strokeWidth="1.5" />
              <circle cx="12" cy="12" r="5" stroke="white" strokeWidth="1.5" />
              <circle cx="12" cy="12" r="1.5" fill="white" />
              <line x1="12" y1="3" x2="12" y2="1" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="12" y1="23" x2="12" y2="21" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="3" y1="12" x2="1" y2="12" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="23" y1="12" x2="21" y2="12" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </div>
          <div>
            <h1 className="text-lg font-bold text-white">Sentinel</h1>
            <p className="text-xs text-gray-500">AI Governance</p>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => (
          <button
            key={item.id}
            onClick={() => onNavigate(item.id)}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all ${
              activePage === item.id
                ? 'bg-sentinel-600/20 text-sentinel-400 border border-sentinel-700/50'
                : 'text-gray-400 hover:bg-gray-800 hover:text-gray-200'
            }`}
          >
            {item.icon}
            {item.label}
          </button>
        ))}
      </nav>

      {/* Status */}
      <div className="p-4 border-t border-gray-800">
        <div className="flex items-center gap-2 px-4 py-2">
          <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
          <span className="text-xs text-gray-500">System Operational</span>
        </div>
      </div>
    </aside>
  )
}

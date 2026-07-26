import { useState } from 'react'
import Landing from './components/Landing'
import LiveDemo from './components/LiveDemo'
import Sidebar from './components/Sidebar'
import Dashboard from './components/Dashboard'
import Agents from './components/Agents'
import Anomalies from './components/Anomalies'
import Policies from './components/Policies'

type View = 'landing' | 'demo' | 'app'
type Page = 'dashboard' | 'agents' | 'anomalies' | 'policies'

function App() {
  const [view, setView] = useState<View>('landing')
  const [page, setPage] = useState<Page>('dashboard')

  if (view === 'landing') {
    return <Landing onStartDemo={() => setView('demo')} onSkipToApp={() => setView('app')} />
  }

  if (view === 'demo') {
    return <LiveDemo onFinish={() => setView('app')} />
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar activePage={page} onNavigate={setPage} />
      <main className="flex-1 overflow-y-auto bg-gray-950 p-8">
        {page === 'dashboard' && <Dashboard />}
        {page === 'agents' && <Agents />}
        {page === 'anomalies' && <Anomalies />}
        {page === 'policies' && <Policies />}
      </main>
    </div>
  )
}

export default App

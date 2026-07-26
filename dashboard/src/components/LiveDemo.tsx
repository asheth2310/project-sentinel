import { useState, useEffect, useRef } from 'react'
import { Shield, ArrowRight, CheckCircle, AlertTriangle, XCircle, Zap, Activity } from 'lucide-react'

interface LiveDemoProps {
  onFinish: () => void
}

interface LogEntry {
  id: number
  time: string
  agent: string
  type: 'normal' | 'warning' | 'critical' | 'system' | 'kill'
  message: string
}

export default function LiveDemo({ onFinish }: LiveDemoProps) {
  const [phase, setPhase] = useState(0)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [tokenCount, setTokenCount] = useState(0)
  const [costCount, setCostCount] = useState(0)
  const [loopCount, setLoopCount] = useState(0)
  const [isKilled, setIsKilled] = useState(false)
  const [showResult, setShowResult] = useState(false)
  const logRef = useRef<HTMLDivElement>(null)
  const idRef = useRef(0)

  const addLog = (agent: string, type: LogEntry['type'], message: string) => {
    idRef.current++
    const now = new Date()
    const time = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`
    setLogs(prev => [...prev, { id: idRef.current, time, agent, type, message }])
  }

  // Auto-scroll logs
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight
    }
  }, [logs])

  // Phase 0: Normal agents reporting
  useEffect(() => {
    if (phase !== 0) return
    const agents = ['agent-support-01', 'agent-code-03', 'agent-research-07', 'agent-data-15']
    let count = 0

    const interval = setInterval(() => {
      const agent = agents[Math.floor(Math.random() * agents.length)]
      const tokens = Math.floor(Math.random() * 2000) + 500
      const cost = (tokens * 0.000003).toFixed(4)
      setTokenCount(prev => prev + tokens)
      setCostCount(prev => prev + parseFloat(cost))
      addLog(agent, 'normal', `Reported ${tokens} tokens ($${cost}) — tool: ${['web_search', 'code_gen', 'summarize', 'analyze'][Math.floor(Math.random() * 4)]}`)
      count++
      if (count >= 8) {
        clearInterval(interval)
        setTimeout(() => setPhase(1), 1000)
      }
    }, 600)

    return () => clearInterval(interval)
  }, [phase])

  // Phase 1: Agent goes rogue (infinite loop)
  useEffect(() => {
    if (phase !== 1) return
    addLog('SENTINEL', 'system', '⚡ Monitoring active — watching for anomalies...')
    
    let count = 0
    const interval = setInterval(() => {
      count++
      setLoopCount(count)
      const tokens = 3000 + count * 500
      const cost = (tokens * 0.000003).toFixed(4)
      setTokenCount(prev => prev + tokens)
      setCostCount(prev => prev + parseFloat(cost))
      addLog('agent-research-07', 'warning', `web_search called again (${count} consecutive) — ${tokens} tokens ($${cost})`)

      if (count === 5) {
        addLog('SENTINEL', 'system', `⚠️ SOFT WARNING: agent-research-07 has ${count} consecutive identical calls (threshold: 10)`)
      }

      if (count >= 10) {
        clearInterval(interval)
        setTimeout(() => setPhase(2), 500)
      }
    }, 500)

    return () => clearInterval(interval)
  }, [phase])

  // Phase 2: Sentinel detects and kills
  useEffect(() => {
    if (phase !== 2) return
    
    addLog('SENTINEL', 'critical', '🚨 ANOMALY DETECTED: INFINITE_LOOP — agent-research-07 made 10 consecutive identical tool calls')
    
    setTimeout(() => {
      addLog('SENTINEL', 'critical', '📊 Severity: CRITICAL (10/10 threshold exceeded)')
    }, 600)

    setTimeout(() => {
      addLog('SENTINEL', 'critical', '🏛️ Governance policy triggered — hard limit reached, auto_kill_enabled=true')
    }, 1200)

    setTimeout(() => {
      addLog('SENTINEL', 'kill', '🛑 CIRCUIT BREAKER ACTIVATED — agent-research-07 is now KILLED')
      setIsKilled(true)
    }, 1800)

    setTimeout(() => {
      addLog('SENTINEL', 'system', '📱 Notification sent to #ai-alerts (Slack) and AI Ops Team (PagerDuty)')
    }, 2400)

    setTimeout(() => {
      addLog('SENTINEL', 'system', '✅ Agent blocked. All further requests will receive HTTP 429.')
      setShowResult(true)
    }, 3000)

  }, [phase])

  return (
    <div className="min-h-screen bg-gray-950 p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-gradient-to-br from-sentinel-500 to-purple-600 rounded-xl flex items-center justify-center shadow-md shadow-sentinel-600/20">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
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
              <h1 className="text-xl font-bold text-white">Live Demo</h1>
              <p className="text-sm text-gray-500">Watch Sentinel detect and stop a rogue agent</p>
            </div>
          </div>
          {showResult && (
            <button
              onClick={onFinish}
              className="flex items-center gap-2 px-5 py-2.5 bg-sentinel-600 hover:bg-sentinel-500 text-white rounded-lg text-sm font-medium transition-all"
            >
              Go to Dashboard
              <ArrowRight size={16} />
            </button>
          )}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Live Stats */}
          <div className="space-y-4">
            <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
              <div className="flex items-center gap-2 mb-3">
                <Activity size={16} className="text-sentinel-400" />
                <span className="text-sm text-gray-400">Total Tokens</span>
              </div>
              <p className="text-2xl font-bold text-white">{tokenCount.toLocaleString()}</p>
            </div>

            <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
              <div className="flex items-center gap-2 mb-3">
                <Zap size={16} className="text-yellow-400" />
                <span className="text-sm text-gray-400">Cost</span>
              </div>
              <p className="text-2xl font-bold text-white">${costCount.toFixed(4)}</p>
            </div>

            <div className={`bg-gray-900 border rounded-xl p-5 transition-colors ${loopCount >= 10 ? 'border-red-800 bg-red-950/20' : loopCount >= 5 ? 'border-amber-800 bg-amber-950/10' : 'border-gray-800'}`}>
              <div className="flex items-center gap-2 mb-3">
                <AlertTriangle size={16} className={loopCount >= 10 ? 'text-red-400' : loopCount >= 5 ? 'text-amber-400' : 'text-gray-400'} />
                <span className="text-sm text-gray-400">Loop Counter</span>
              </div>
              <p className={`text-2xl font-bold ${loopCount >= 10 ? 'text-red-400' : loopCount >= 5 ? 'text-amber-400' : 'text-white'}`}>
                {loopCount} / 10
              </p>
              <div className="mt-2 h-2 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-300 ${loopCount >= 10 ? 'bg-red-500' : loopCount >= 5 ? 'bg-amber-500' : 'bg-sentinel-500'}`}
                  style={{ width: `${Math.min(loopCount * 10, 100)}%` }}
                />
              </div>
            </div>

            {/* Agent Status */}
            <div className={`border rounded-xl p-5 transition-all ${isKilled ? 'bg-red-950/30 border-red-800' : 'bg-gray-900 border-gray-800'}`}>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-400">agent-research-07</span>
                {isKilled ? (
                  <span className="inline-flex items-center gap-1 px-2.5 py-1 bg-red-900/50 text-red-400 rounded-full text-xs font-medium border border-red-800">
                    <XCircle size={12} /> KILLED
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 px-2.5 py-1 bg-emerald-900/50 text-emerald-400 rounded-full text-xs font-medium border border-emerald-800">
                    <CheckCircle size={12} /> Active
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Live Log Feed */}
          <div className="lg:col-span-2 bg-gray-900 border border-gray-800 rounded-xl overflow-hidden flex flex-col" style={{ height: '600px' }}>
            <div className="px-5 py-3 border-b border-gray-800 flex items-center justify-between">
              <span className="text-sm font-medium text-gray-300">Live Event Log</span>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
                <span className="text-xs text-gray-500">Streaming</span>
              </div>
            </div>
            <div ref={logRef} className="flex-1 overflow-y-auto p-4 space-y-1 font-mono text-xs">
              {logs.map((log) => (
                <div key={log.id} className={`flex gap-3 py-1 px-2 rounded ${
                  log.type === 'critical' ? 'bg-red-950/20' :
                  log.type === 'kill' ? 'bg-red-950/40' :
                  log.type === 'warning' ? 'bg-amber-950/10' :
                  log.type === 'system' ? 'bg-sentinel-950/20' :
                  ''
                }`}>
                  <span className="text-gray-600 flex-shrink-0">{log.time}</span>
                  <span className={`flex-shrink-0 w-36 ${
                    log.type === 'critical' || log.type === 'kill' ? 'text-red-400' :
                    log.type === 'warning' ? 'text-amber-400' :
                    log.type === 'system' ? 'text-sentinel-400' :
                    'text-gray-500'
                  }`}>
                    [{log.agent}]
                  </span>
                  <span className={`${
                    log.type === 'critical' || log.type === 'kill' ? 'text-red-300' :
                    log.type === 'warning' ? 'text-amber-300' :
                    log.type === 'system' ? 'text-sentinel-300' :
                    'text-gray-400'
                  }`}>
                    {log.message}
                  </span>
                </div>
              ))}
              {!showResult && (
                <div className="flex items-center gap-2 py-1 px-2 text-gray-600">
                  <div className="w-1.5 h-1.5 bg-gray-600 rounded-full animate-pulse" />
                  <span>Waiting for events...</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Result Summary */}
        {showResult && (
          <div className="mt-8 bg-emerald-950/20 border border-emerald-800/50 rounded-xl p-6">
            <div className="flex items-start gap-4">
              <div className="w-10 h-10 bg-emerald-900/40 rounded-full flex items-center justify-center flex-shrink-0">
                <CheckCircle size={20} className="text-emerald-400" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-emerald-300">Threat Neutralized</h3>
                <p className="text-sm text-gray-400 mt-1">
                  Sentinel detected an infinite loop after 10 consecutive identical calls, 
                  activated the circuit breaker, and notified the team — all in under 6 seconds.
                  Without Sentinel, this agent would have continued burning tokens indefinitely.
                </p>
                <div className="flex gap-6 mt-4">
                  <div>
                    <p className="text-xs text-gray-500">Tokens saved (estimated)</p>
                    <p className="text-lg font-bold text-emerald-400">~500,000+</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500">Cost saved</p>
                    <p className="text-lg font-bold text-emerald-400">~$15.00+</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500">Detection time</p>
                    <p className="text-lg font-bold text-emerald-400">&lt; 6 seconds</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

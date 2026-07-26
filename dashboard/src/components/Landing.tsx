import { Shield, Zap, AlertTriangle, Activity, DollarSign, Bot, ArrowRight, Play } from 'lucide-react'

interface LandingProps {
  onStartDemo: () => void
  onSkipToApp: () => void
}

export default function Landing({ onStartDemo, onSkipToApp }: LandingProps) {
  return (
    <div className="min-h-screen bg-gray-950 overflow-y-auto">
      {/* Hero */}
      <section className="relative px-6 py-20 flex flex-col items-center text-center">
        <div className="absolute inset-0 bg-gradient-to-b from-sentinel-950/50 to-transparent pointer-events-none" />
        
        <div className="relative z-10">
          <div className="w-16 h-16 bg-gradient-to-br from-sentinel-500 to-purple-600 rounded-2xl flex items-center justify-center mx-auto mb-6 shadow-lg shadow-sentinel-600/30">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <circle cx="12" cy="12" r="9" stroke="white" strokeWidth="1.5" />
              <circle cx="12" cy="12" r="5" stroke="white" strokeWidth="1.5" />
              <circle cx="12" cy="12" r="1.5" fill="white" />
              <line x1="12" y1="3" x2="12" y2="1" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="12" y1="23" x2="12" y2="21" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="3" y1="12" x2="1" y2="12" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="23" y1="12" x2="21" y2="12" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </div>
          
          <h1 className="text-5xl font-bold text-white mb-4">
            Project Sentinel
          </h1>
          <p className="text-xl text-gray-400 max-w-2xl mx-auto mb-8">
            A monitoring & kill-switch system for AI agents. <br />
            Detects runaway costs, infinite loops, and token explosions — then stops them automatically.
          </p>

          <div className="flex gap-4 justify-center">
            <button
              onClick={onStartDemo}
              className="flex items-center gap-2 px-6 py-3 bg-sentinel-600 hover:bg-sentinel-500 text-white rounded-xl text-base font-medium transition-all shadow-lg shadow-sentinel-600/20"
            >
              <Play size={18} />
              Watch Live Demo
            </button>
            <button
              onClick={onSkipToApp}
              className="flex items-center gap-2 px-6 py-3 bg-gray-800 hover:bg-gray-700 text-gray-200 rounded-xl text-base font-medium transition-all border border-gray-700"
            >
              Skip to Dashboard
              <ArrowRight size={18} />
            </button>
          </div>
        </div>
      </section>

      {/* What Problem */}
      <section className="px-6 py-16 max-w-5xl mx-auto">
        <h2 className="text-3xl font-bold text-white text-center mb-4">The Problem</h2>
        <p className="text-gray-400 text-center max-w-2xl mx-auto mb-12">
          Companies run dozens of AI agents 24/7. When one breaks, it burns money silently until someone notices.
        </p>

        <div className="grid md:grid-cols-3 gap-6">
          <ProblemCard
            icon={<Zap className="text-red-400" size={24} />}
            title="Token Explosion"
            description="A recursive prompt generates exponential tokens. Your $10/hour agent becomes $10,000/hour in seconds."
            cost="$10,000+ lost"
          />
          <ProblemCard
            icon={<AlertTriangle className="text-orange-400" size={24} />}
            title="Infinite Loops"
            description="Agent gets stuck calling the same tool over and over. Nobody notices for hours."
            cost="$5,000+ wasted"
          />
          <ProblemCard
            icon={<DollarSign className="text-yellow-400" size={24} />}
            title="Cost Spikes"
            description="A sudden surge in usage — maybe a bug, maybe an attack. You find out on the invoice."
            cost="$50,000 surprise bill"
          />
        </div>
      </section>

      {/* How It Works */}
      <section className="px-6 py-16 max-w-5xl mx-auto">
        <h2 className="text-3xl font-bold text-white text-center mb-4">How Sentinel Works</h2>
        <p className="text-gray-400 text-center max-w-2xl mx-auto mb-12">
          Three steps. Real-time. Fully automatic.
        </p>

        <div className="grid md:grid-cols-3 gap-8">
          <StepCard
            step="1"
            icon={<Activity className="text-sentinel-400" size={24} />}
            title="Track"
            description="Every AI agent reports what it did — tokens used, cost, latency, which tools it called. Like a fitness tracker for bots."
          />
          <StepCard
            step="2"
            icon={<AlertTriangle className="text-sentinel-400" size={24} />}
            title="Detect"
            description="Sentinel watches for bad patterns in real-time: loops, cascades, unusual spikes. Uses statistical analysis (Z-scores, growth rates)."
          />
          <StepCard
            step="3"
            icon={<Shield className="text-sentinel-400" size={24} />}
            title="Stop"
            description="When danger is detected: warns your team at 80% threshold, automatically kills the agent at 100%. Alerts go to Slack/PagerDuty."
          />
        </div>
      </section>

      {/* What the Data Means */}
      <section className="px-6 py-16 max-w-5xl mx-auto">
        <h2 className="text-3xl font-bold text-white text-center mb-4">Understanding the Data</h2>
        <p className="text-gray-400 text-center max-w-2xl mx-auto mb-12">
          Here's what each number on the dashboard represents:
        </p>

        <div className="grid md:grid-cols-2 gap-4">
          <DataExplainer
            term="Tokens"
            explanation="The currency of AI. Every word you send to GPT costs tokens. 1,000 tokens ≈ 750 words ≈ $0.01–$0.06"
          />
          <DataExplainer
            term="Cost (24h)"
            explanation="Total money spent on AI API calls in the last 24 hours across all your agents"
          />
          <DataExplainer
            term="Events/min"
            explanation="How many telemetry reports your agents are sending per minute. Higher = more active agents"
          />
          <DataExplainer
            term="Anomalies"
            explanation="Things that look wrong — unusual spikes, loops, or cascades detected by Sentinel's algorithms"
          />
          <DataExplainer
            term="Circuit Breaker"
            explanation="A kill-switch. When activated, the agent is blocked from making any more API calls until you review it"
          />
          <DataExplainer
            term="Z-score"
            explanation="How unusual something is statistically. Z-score of 3 = this event is more extreme than 99.7% of normal behavior"
          />
          <DataExplainer
            term="Sliding Window"
            explanation="Sentinel looks at the last 60 seconds of data. Old data falls off, keeping the view fresh and real-time"
          />
          <DataExplainer
            term="Governance Policy"
            explanation="Your rules. 'Kill any agent spending >$10/hour' or 'Warn me if an agent loops 5+ times'"
          />
        </div>
      </section>

      {/* CTA */}
      <section className="px-6 py-20 text-center">
        <button
          onClick={onStartDemo}
          className="inline-flex items-center gap-2 px-8 py-4 bg-sentinel-600 hover:bg-sentinel-500 text-white rounded-xl text-lg font-medium transition-all shadow-lg shadow-sentinel-600/20"
        >
          <Play size={20} />
          Watch the Live Demo
        </button>
        <p className="text-gray-600 mt-4 text-sm">See Sentinel catch a rogue agent in real-time</p>
      </section>
    </div>
  )
}

function ProblemCard({ icon, title, description, cost }: { icon: React.ReactNode, title: string, description: string, cost: string }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <div className="mb-4">{icon}</div>
      <h3 className="text-lg font-semibold text-white mb-2">{title}</h3>
      <p className="text-sm text-gray-400 mb-4">{description}</p>
      <p className="text-xs font-medium text-red-400">{cost}</p>
    </div>
  )
}

function StepCard({ step, icon, title, description }: { step: string, icon: React.ReactNode, title: string, description: string }) {
  return (
    <div className="relative bg-gray-900 border border-gray-800 rounded-xl p-6">
      <div className="absolute -top-3 -left-3 w-7 h-7 bg-sentinel-600 rounded-full flex items-center justify-center text-xs font-bold text-white">
        {step}
      </div>
      <div className="mb-4">{icon}</div>
      <h3 className="text-lg font-semibold text-white mb-2">{title}</h3>
      <p className="text-sm text-gray-400">{description}</p>
    </div>
  )
}

function DataExplainer({ term, explanation }: { term: string, explanation: string }) {
  return (
    <div className="flex gap-4 p-4 bg-gray-900/50 border border-gray-800/50 rounded-lg">
      <div className="flex-shrink-0 w-2 h-2 bg-sentinel-500 rounded-full mt-2" />
      <div>
        <p className="text-sm font-medium text-white">{term}</p>
        <p className="text-xs text-gray-500 mt-0.5">{explanation}</p>
      </div>
    </div>
  )
}

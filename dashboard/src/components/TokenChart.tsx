import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

const data = [
  { time: '00:00', prompt: 45000, completion: 32000 },
  { time: '02:00', prompt: 38000, completion: 28000 },
  { time: '04:00', prompt: 22000, completion: 15000 },
  { time: '06:00', prompt: 35000, completion: 24000 },
  { time: '08:00', prompt: 78000, completion: 55000 },
  { time: '10:00', prompt: 95000, completion: 68000 },
  { time: '12:00', prompt: 112000, completion: 82000 },
  { time: '14:00', prompt: 98000, completion: 71000 },
  { time: '16:00', prompt: 125000, completion: 89000 },
  { time: '18:00', prompt: 140000, completion: 95000 },
  { time: '20:00', prompt: 88000, completion: 62000 },
  { time: '22:00', prompt: 65000, completion: 45000 },
]

export default function TokenChart() {
  return (
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={data} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="promptGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#0c86f0" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#0c86f0" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="completionGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
        <XAxis dataKey="time" stroke="#6b7280" fontSize={12} />
        <YAxis stroke="#6b7280" fontSize={12} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
        <Tooltip
          contentStyle={{
            backgroundColor: '#111827',
            border: '1px solid #374151',
            borderRadius: '8px',
            fontSize: '12px',
          }}
          labelStyle={{ color: '#9ca3af' }}
        />
        <Area
          type="monotone"
          dataKey="prompt"
          stroke="#0c86f0"
          strokeWidth={2}
          fill="url(#promptGradient)"
          name="Prompt Tokens"
        />
        <Area
          type="monotone"
          dataKey="completion"
          stroke="#8b5cf6"
          strokeWidth={2}
          fill="url(#completionGradient)"
          name="Completion Tokens"
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}

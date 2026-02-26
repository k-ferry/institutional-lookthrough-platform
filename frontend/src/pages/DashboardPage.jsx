import { useQuery } from '@tanstack/react-query'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  DollarSign,
  Briefcase,
  Building2,
  PieChart,
  AlertCircle,
} from 'lucide-react'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatAUM(value) {
  if (value === null || value === undefined) return '—'
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`
  return `$${value.toFixed(0)}`
}

function formatNumber(n) {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
}

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

// ---------------------------------------------------------------------------
// Skeleton
// ---------------------------------------------------------------------------

function StatSkeleton() {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-2 flex-1">
            <div className="h-3 w-24 bg-secondary-200 rounded animate-pulse" />
            <div className="h-7 w-32 bg-secondary-200 rounded animate-pulse" />
            <div className="h-2 w-28 bg-secondary-100 rounded animate-pulse" />
          </div>
          <div className="h-12 w-12 rounded-full bg-secondary-100 animate-pulse" />
        </div>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Stat Card
// ---------------------------------------------------------------------------

function StatCard({ title, value, icon: Icon, description }) {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium text-secondary-500">{title}</p>
            <p className="text-2xl font-bold text-secondary-900 mt-1">{value}</p>
            <p className="text-xs text-secondary-400 mt-1">{description}</p>
          </div>
          <div className="h-12 w-12 rounded-full bg-primary-50 flex items-center justify-center">
            <Icon className="h-6 w-6 text-primary-600" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Error Banner
// ---------------------------------------------------------------------------

function ErrorBanner({ message }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <AlertCircle className="h-4 w-4 shrink-0" />
      <span>{message}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sector Chart
// ---------------------------------------------------------------------------

const CHART_COLOR = '#1A4B9B' // primary-500

function SectorChart({ sectors }) {
  const data = sectors.map((s) => ({
    name: s.sector.length > 20 ? s.sector.slice(0, 18) + '…' : s.sector,
    fullName: s.sector,
    value: s.holding_count,
    total_value: s.total_value,
    pct: s.percentage,
  }))

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 32, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
        <XAxis
          type="number"
          tickFormatter={(v) => formatNumber(v)}
          tick={{ fontSize: 11, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
          label={{ value: 'Holdings', position: 'insideBottomRight', offset: -4, fontSize: 10, fill: '#94a3b8' }}
        />
        <YAxis
          type="category"
          dataKey="name"
          width={130}
          tick={{ fontSize: 11, fill: '#475569' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value, _name, props) => {
            const { total_value, pct, fullName } = props.payload
            const valueStr = total_value != null ? ` · ${formatAUM(total_value)}` : ''
            return [`${formatNumber(value)} holdings${valueStr} (${pct}%)`, fullName]
          }}
          contentStyle={{
            fontSize: 12,
            borderRadius: 6,
            border: '1px solid #e2e8f0',
          }}
        />
        <Bar dataKey="value" fill={CHART_COLOR} radius={[0, 4, 4, 0]} maxBarSize={24} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Fund Table
// ---------------------------------------------------------------------------

function FundTable({ funds }) {
  return (
    <div className="overflow-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-secondary-200">
            <th className="text-left py-2 px-1 font-medium text-secondary-500">Fund</th>
            <th className="text-right py-2 px-1 font-medium text-secondary-500">Holdings</th>
            <th className="text-right py-2 px-1 font-medium text-secondary-500">AUM</th>
            <th className="text-right py-2 px-1 font-medium text-secondary-500">% Portfolio</th>
          </tr>
        </thead>
        <tbody>
          {funds.map((f) => (
            <tr key={f.fund_id} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
              <td className="py-2 px-1 text-secondary-800 font-medium max-w-[180px] truncate">
                {f.fund_name}
              </td>
              <td className="py-2 px-1 text-right text-secondary-600 tabular-nums">
                {formatNumber(f.holding_count ?? 0)}
              </td>
              <td className="py-2 px-1 text-right text-secondary-800 font-medium tabular-nums">
                {f.total_value != null ? formatAUM(f.total_value) : 'N/A'}
              </td>
              <td className="py-2 px-1 text-right tabular-nums">
                <span className="inline-flex items-center rounded-full bg-primary-50 px-2 py-0.5 text-xs font-medium text-primary-700">
                  {(f.percentage_of_portfolio ?? 0).toFixed(1)}%
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function DashboardPage() {
  const {
    data: stats,
    isLoading: statsLoading,
    error: statsError,
  } = useQuery({
    queryKey: ['dashboard-stats'],
    queryFn: () => fetchJSON('/api/dashboard/stats'),
  })

  const {
    data: sectorData,
    isLoading: sectorsLoading,
    error: sectorsError,
  } = useQuery({
    queryKey: ['dashboard-sectors'],
    queryFn: () => fetchJSON('/api/dashboard/sector-breakdown'),
  })

  const {
    data: fundData,
    isLoading: fundsLoading,
    error: fundsError,
  } = useQuery({
    queryKey: ['dashboard-funds'],
    queryFn: () => fetchJSON('/api/dashboard/fund-breakdown'),
  })

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Portfolio Overview</h1>
        <p className="text-secondary-500 mt-1">
          Aggregated view across all institutional holdings
        </p>
      </div>

      {statsError && (
        <ErrorBanner message={`Failed to load stats: ${statsError.message}`} />
      )}

      {/* Stat Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {statsLoading ? (
          <>
            <StatSkeleton />
            <StatSkeleton />
            <StatSkeleton />
            <StatSkeleton />
          </>
        ) : stats ? (
          <>
            <StatCard
              title="Total AUM"
              value={formatAUM(stats.total_aum)}
              icon={DollarSign}
              description="Assets under management"
            />
            <StatCard
              title="Holdings"
              value={formatNumber(stats.total_holdings)}
              icon={Briefcase}
              description="Individual positions"
            />
            <StatCard
              title="Companies"
              value={formatNumber(stats.total_companies)}
              icon={Building2}
              description="Unique entities"
            />
            <StatCard
              title="Funds"
              value={formatNumber(stats.total_funds)}
              icon={PieChart}
              description={`${stats.data_sources} data source${stats.data_sources !== 1 ? 's' : ''}`}
            />
          </>
        ) : null}
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Sector Breakdown */}
        <Card>
          <CardHeader>
            <CardTitle>Sector Allocation</CardTitle>
          </CardHeader>
          <CardContent>
            {sectorsError ? (
              <ErrorBanner message={`Failed to load sectors: ${sectorsError.message}`} />
            ) : sectorsLoading ? (
              <div className="h-64 flex items-center justify-center">
                <div className="space-y-3 w-full px-4">
                  {[...Array(5)].map((_, i) => (
                    <div key={i} className="flex items-center gap-3">
                      <div className="h-3 w-28 bg-secondary-200 rounded animate-pulse" />
                      <div
                        className="h-5 bg-secondary-200 rounded animate-pulse"
                        style={{ width: `${60 - i * 10}%` }}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : sectorData?.sectors?.length ? (
              <SectorChart sectors={sectorData.sectors} />
            ) : (
              <div className="h-64 flex items-center justify-center text-secondary-400 text-sm">
                No sector data available
              </div>
            )}
          </CardContent>
        </Card>

        {/* Fund Breakdown */}
        <Card>
          <CardHeader>
            <CardTitle>Fund Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            {fundsError ? (
              <ErrorBanner message={`Failed to load funds: ${fundsError.message}`} />
            ) : fundsLoading ? (
              <div className="space-y-3">
                {[...Array(5)].map((_, i) => (
                  <div key={i} className="flex justify-between items-center py-1">
                    <div className="h-3 w-40 bg-secondary-200 rounded animate-pulse" />
                    <div className="h-3 w-16 bg-secondary-200 rounded animate-pulse" />
                    <div className="h-3 w-20 bg-secondary-200 rounded animate-pulse" />
                  </div>
                ))}
              </div>
            ) : fundData?.funds?.length ? (
              <FundTable funds={fundData.funds} />
            ) : (
              <div className="h-64 flex items-center justify-center text-secondary-400 text-sm">
                No fund data available
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

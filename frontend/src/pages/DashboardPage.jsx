import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  DollarSign,
  Briefcase,
  Building2,
  BarChart2,
  CheckCircle,
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
// Source metadata
// ---------------------------------------------------------------------------

const SOURCE_META = {
  synthetic:    {
    label: 'Synthetic',
    groupLabel: 'Synthetic Data',
    badgeClasses: 'bg-gray-100 text-gray-700 border-gray-200',
    accentBg: 'bg-gray-50',
    accentBorder: 'border-l-gray-400',
    headerBg: 'bg-gray-50',
  },
  bdc_filing:   {
    label: 'BDC Filing',
    groupLabel: 'BDC Filings',
    badgeClasses: 'bg-blue-100 text-blue-800 border-blue-200',
    accentBg: 'bg-blue-50',
    accentBorder: 'border-l-blue-500',
    headerBg: 'bg-blue-50',
  },
  '13f_filing': {
    label: '13F Filing',
    groupLabel: 'Public Market Filings',
    badgeClasses: 'bg-green-100 text-green-800 border-green-200',
    accentBg: 'bg-green-50',
    accentBorder: 'border-l-green-500',
    headerBg: 'bg-green-50',
  },
  pdf_document: {
    label: 'PDF Document',
    groupLabel: 'Private Market Funds',
    badgeClasses: 'bg-purple-100 text-purple-800 border-purple-200',
    accentBg: 'bg-purple-50',
    accentBorder: 'border-l-purple-500',
    headerBg: 'bg-purple-50',
  },
}

// Fund type badge colors
const FUND_TYPE_CLASSES = {
  PE:        'bg-blue-900 text-white',
  VC:        'bg-blue-600 text-white',
  hedge:     'bg-purple-700 text-white',
  credit:    'bg-teal-600 text-white',
  BDC:       'bg-emerald-600 text-white',
  ETF:       'bg-sky-200 text-sky-900',
  synthetic: 'bg-gray-400 text-white',
}

function fundTypeBadgeClasses(type) {
  if (!type) return 'bg-gray-200 text-gray-600'
  for (const [key, cls] of Object.entries(FUND_TYPE_CLASSES)) {
    if (type.toLowerCase() === key.toLowerCase()) return cls
  }
  return 'bg-gray-200 text-gray-600'
}

// Fund table group order
const SOURCE_GROUP_ORDER = ['pdf_document', '13f_filing', 'bdc_filing', 'synthetic']

// Trend chart palette
const TREND_COLORS = [
  '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#84cc16',
]

// ---------------------------------------------------------------------------
// Skeletons
// ---------------------------------------------------------------------------

function StatCardSkeleton() {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-2 flex-1">
            <div className="h-3 w-20 bg-secondary-200 rounded animate-pulse" />
            <div className="h-7 w-28 bg-secondary-200 rounded animate-pulse" />
            <div className="h-2.5 w-24 bg-secondary-100 rounded animate-pulse" />
          </div>
          <div className="h-12 w-12 rounded-full bg-secondary-100 animate-pulse" />
        </div>
      </CardContent>
    </Card>
  )
}

function SourceCardSkeleton() {
  return (
    <div className="rounded-lg border border-secondary-200 bg-white p-4 border-l-4 border-l-secondary-200 animate-pulse">
      <div className="h-4 w-24 bg-secondary-200 rounded mb-3" />
      <div className="flex justify-between items-end">
        <div className="h-8 w-16 bg-secondary-200 rounded" />
        <div className="h-5 w-10 bg-secondary-100 rounded" />
      </div>
    </div>
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
          <div className="min-w-0">
            <p className="text-sm font-medium text-secondary-500">{title}</p>
            <p className="text-2xl font-bold text-secondary-900 mt-1 truncate">{value ?? '—'}</p>
            {description && <p className="text-xs text-secondary-400 mt-1">{description}</p>}
          </div>
          <div className="h-12 w-12 rounded-full bg-primary-50 flex items-center justify-center shrink-0 ml-3">
            <Icon className="h-6 w-6 text-primary-600" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Source Breakdown Card
// ---------------------------------------------------------------------------

function SourceCard({ source, holdingCount, fundCount, latestDate }) {
  const meta = SOURCE_META[source] ?? {
    label: source,
    badgeClasses: 'bg-gray-100 text-gray-700 border-gray-200',
    accentBorder: 'border-l-gray-300',
  }
  return (
    <div className={`rounded-lg border border-secondary-200 bg-white p-4 border-l-4 ${meta.accentBorder}`}>
      <div className="flex items-start justify-between mb-3">
        <span
          className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${meta.badgeClasses}`}
        >
          {meta.label}
        </span>
        <span className="text-xs text-secondary-400 ml-2 shrink-0">{latestDate ?? '—'}</span>
      </div>
      <div className="flex items-end justify-between">
        <div>
          <p className="text-2xl font-bold text-secondary-900 tabular-nums">
            {holdingCount?.toLocaleString() ?? '—'}
          </p>
          <p className="text-xs text-secondary-500 mt-0.5">holdings</p>
        </div>
        <div className="text-right">
          <p className="text-lg font-semibold text-secondary-700 tabular-nums">{fundCount ?? '—'}</p>
          <p className="text-xs text-secondary-500">fund{fundCount !== 1 ? 's' : ''}</p>
        </div>
      </div>
    </div>
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
// Exposure Trend Chart
// ---------------------------------------------------------------------------

function ExposureTrendChart({ data, loading }) {
  if (loading) {
    return (
      <div className="h-72 flex items-center justify-center">
        <div className="space-y-3 w-full px-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="h-10 bg-secondary-200 rounded animate-pulse" />
          ))}
        </div>
      </div>
    )
  }

  if (!data?.dates?.length || !data?.series?.length) {
    return (
      <div className="h-72 flex items-center justify-center text-secondary-400 text-sm">
        No trend data available. Run the pipeline to generate snapshots.
      </div>
    )
  }

  const chartData = data.dates.map((d, i) => {
    const point = { date: d }
    data.series.forEach((s) => { point[s.name] = s.data[i] ?? 0 })
    return point
  })

  return (
    <ResponsiveContainer width="100%" height={300}>
      <AreaChart data={chartData} margin={{ left: 8, right: 24, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 10, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tickFormatter={(v) => `${v.toFixed(0)}%`}
          tick={{ fontSize: 10, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
          domain={[0, 100]}
        />
        <Tooltip
          formatter={(value, name) => [`${Number(value).toFixed(1)}%`, name]}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        {data.series.map((s, i) => (
          <Area
            key={s.name}
            type="monotone"
            dataKey={s.name}
            stackId="1"
            stroke={TREND_COLORS[i % TREND_COLORS.length]}
            fill={TREND_COLORS[i % TREND_COLORS.length]}
            fillOpacity={0.7}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Fund Lineup Table
// ---------------------------------------------------------------------------

function FundLineupTable({ funds, loading }) {
  if (loading) {
    return (
      <div className="space-y-1 p-2">
        {[...Array(8)].map((_, i) => (
          <div key={i} className="flex items-center gap-4 p-3">
            <div className="h-3.5 w-48 bg-secondary-200 rounded animate-pulse" />
            <div className="h-5 w-16 bg-secondary-100 rounded-full animate-pulse" />
            <div className="h-3 w-12 bg-secondary-100 rounded animate-pulse ml-auto" />
            <div className="h-3 w-20 bg-secondary-100 rounded animate-pulse" />
          </div>
        ))}
      </div>
    )
  }

  if (!funds?.length) {
    return (
      <div className="py-12 text-center text-secondary-400 text-sm">
        No fund data available
      </div>
    )
  }

  // Group by source, preserving order
  const grouped = {}
  SOURCE_GROUP_ORDER.forEach((s) => { grouped[s] = [] })
  funds.forEach((f) => {
    const key = SOURCE_GROUP_ORDER.includes(f.source) ? f.source : 'synthetic'
    grouped[key].push(f)
  })

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-secondary-200 bg-secondary-50">
            <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Fund</th>
            <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Type</th>
            <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Holdings</th>
            <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Exposure</th>
            <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Sectors</th>
            <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">As of Date</th>
          </tr>
        </thead>
        <tbody>
          {SOURCE_GROUP_ORDER.flatMap((source) => {
            const group = grouped[source]
            if (!group?.length) return []
            const meta = SOURCE_META[source]
            const groupLabel = meta?.groupLabel ?? source

            return [
              // Group header row
              <tr key={`hdr-${source}`}>
                <td
                  colSpan={6}
                  className={`py-2 px-4 border-y border-secondary-100 ${meta?.headerBg ?? 'bg-secondary-50'}`}
                >
                  <div className="flex items-center gap-2.5">
                    <span
                      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${meta?.badgeClasses ?? ''}`}
                    >
                      {meta?.label ?? source}
                    </span>
                    <span className="text-xs font-semibold text-secondary-600 uppercase tracking-wide">
                      {groupLabel}
                    </span>
                    <span className="text-xs text-secondary-400">
                      {group.length} fund{group.length !== 1 ? 's' : ''}
                    </span>
                  </div>
                </td>
              </tr>,
              // Fund rows
              ...group.map((f) => (
                <tr
                  key={f.fund_id}
                  className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors"
                >
                  {/* Fund name */}
                  <td className="py-3 px-4 max-w-[240px]">
                    <Link
                      to={`/funds/${f.fund_id}`}
                      className="font-medium text-primary-700 hover:text-primary-900 hover:underline truncate block"
                      title={f.fund_name}
                    >
                      {f.fund_name}
                    </Link>
                  </td>

                  {/* Fund type badge */}
                  <td className="py-3 px-4">
                    {f.fund_type ? (
                      <span
                        className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${fundTypeBadgeClasses(f.fund_type)}`}
                      >
                        {f.fund_type}
                      </span>
                    ) : (
                      <span className="text-secondary-300 text-xs">—</span>
                    )}
                  </td>

                  {/* Holdings */}
                  <td className="py-3 px-4 text-right tabular-nums text-secondary-700">
                    {formatNumber(f.holding_count)}
                  </td>

                  {/* Exposure */}
                  <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                    {f.total_exposure_usd != null ? formatAUM(f.total_exposure_usd) : (
                      <span className="text-secondary-300 font-normal">—</span>
                    )}
                  </td>

                  {/* Sectors covered */}
                  <td className="py-3 px-4 text-right tabular-nums text-secondary-500">
                    {f.sector_count > 0 ? f.sector_count : (
                      <span className="text-secondary-300">—</span>
                    )}
                  </td>

                  {/* Latest as-of date */}
                  <td className="py-3 px-4 text-secondary-500 text-xs">
                    {f.latest_as_of_date ?? '—'}
                  </td>
                </tr>
              )),
            ]
          })}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function DashboardPage() {
  const [trendDim, setTrendDim] = useState('sector')

  const { data: stats, isLoading: statsLoading, error: statsError } = useQuery({
    queryKey: ['dashboard-stats'],
    queryFn: () => fetchJSON('/api/dashboard/stats'),
    staleTime: 5 * 60 * 1000,
  })

  const { data: trendData, isLoading: trendLoading } = useQuery({
    queryKey: ['exposure-trend', trendDim],
    queryFn: () => fetchJSON(`/api/dashboard/exposure-trend?dimension_type=${trendDim}&periods=8`),
    staleTime: 5 * 60 * 1000,
  })

  const { data: fundsData, isLoading: fundsLoading, error: fundsError } = useQuery({
    queryKey: ['dashboard-funds-summary'],
    queryFn: () => fetchJSON('/api/dashboard/funds-summary'),
    staleTime: 5 * 60 * 1000,
  })

  // Determine if geography data has enough known countries to be worth showing
  const { data: geoBreakdownData } = useQuery({
    queryKey: ['dashboard-geography-breakdown'],
    queryFn: () => fetchJSON('/api/dashboard/geography-breakdown'),
    staleTime: 10 * 60 * 1000,
  })
  const _geoTotal = (geoBreakdownData ?? []).reduce((sum, g) => sum + (g.value_usd ?? 0), 0)
  const _geoKnown = (geoBreakdownData ?? [])
    .filter((g) => g.country !== 'Unknown')
    .reduce((sum, g) => sum + (g.value_usd ?? 0), 0)
  const showGeoToggle = _geoTotal > 0 && _geoKnown / _geoTotal > 0.20

  // If geography toggle disappears while selected, fall back to sector
  useEffect(() => {
    if (!showGeoToggle && trendDim === 'geography') setTrendDim('sector')
  }, [showGeoToggle, trendDim])

  // Source breakdown from stats
  const sourceBreakdown = stats?.source_breakdown ?? []
  const sourceMap = Object.fromEntries(sourceBreakdown.map((s) => [s.source, s]))

  return (
    <div className="space-y-6">

      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Portfolio Overview</h1>
        <p className="text-secondary-500 mt-1 text-sm">
          Aggregated look-through exposure across all institutional holdings
        </p>
      </div>

      {statsError && <ErrorBanner message={`Failed to load stats: ${statsError.message}`} />}

      {/* Top stat cards (5) */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        {statsLoading ? (
          <>
            {[...Array(5)].map((_, i) => <StatCardSkeleton key={i} />)}
          </>
        ) : stats ? (
          <>
            <StatCard
              title="Total Exposure"
              value={formatAUM(stats.total_exposure_usd)}
              icon={DollarSign}
              description="Sum of reported values"
            />
            <StatCard
              title="Holdings"
              value={formatNumber(stats.total_holdings)}
              icon={Briefcase}
              description="Individual positions"
            />
            <StatCard
              title="Funds"
              value={formatNumber(stats.fund_count)}
              icon={BarChart2}
              description={`${stats.data_sources} source${stats.data_sources !== 1 ? 's' : ''}`}
            />
            <StatCard
              title="Companies"
              value={formatNumber(stats.company_count)}
              icon={Building2}
              description="Unique entities"
            />
            <StatCard
              title="Classified"
              value={`${stats.classification_coverage_pct ?? 0}%`}
              icon={CheckCircle}
              description={`of ${formatNumber(stats.company_count)} companies`}
            />
          </>
        ) : null}
      </div>

      {/* Source breakdown (4 cards) */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {statsLoading ? (
          <>
            {[...Array(4)].map((_, i) => <SourceCardSkeleton key={i} />)}
          </>
        ) : (
          SOURCE_GROUP_ORDER.map((source) => {
            const s = sourceMap[source]
            if (!s) return null
            return (
              <SourceCard
                key={source}
                source={source}
                holdingCount={s.holding_count}
                fundCount={s.fund_count}
                latestDate={s.latest_as_of_date}
              />
            )
          })
        )}
      </div>

      {/* Portfolio Exposure Over Time */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Portfolio Exposure Over Time</CardTitle>
              <p className="text-xs text-secondary-400 mt-0.5">
                % allocation by {trendDim} — all sources combined
              </p>
            </div>
            <div className="flex items-center gap-1">
              {['sector', ...(showGeoToggle ? ['geography'] : [])].map((dim) => (
                <button
                  key={dim}
                  onClick={() => setTrendDim(dim)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    trendDim === dim
                      ? 'bg-primary-600 text-white'
                      : 'bg-secondary-100 text-secondary-600 hover:bg-secondary-200'
                  }`}
                >
                  {dim.charAt(0).toUpperCase() + dim.slice(1)}
                </button>
              ))}
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <ExposureTrendChart data={trendData} loading={trendLoading} />
        </CardContent>
      </Card>

      {/* Fund Lineup */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Fund Lineup</CardTitle>
              <p className="text-xs text-secondary-400 mt-0.5">
                {fundsData?.length ?? 0} funds — latest quarter data — sorted by exposure
              </p>
            </div>
            <Link
              to="/holdings"
              className="text-xs text-primary-600 hover:text-primary-800 hover:underline"
            >
              View all holdings →
            </Link>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {fundsError ? (
            <div className="p-4">
              <ErrorBanner message={`Failed to load funds: ${fundsError.message}`} />
            </div>
          ) : (
            <FundLineupTable funds={fundsData} loading={fundsLoading} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

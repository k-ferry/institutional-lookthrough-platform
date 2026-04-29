import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  Cell,
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

// Fund table group order (source cards at top of page)
const SOURCE_GROUP_ORDER = ['pdf_document', '13f_filing', 'bdc_filing', 'synthetic']

// Fund lineup grouping by asset class
const FUND_TYPE_GROUPS = [
  {
    key: 'private_equity',
    label: 'Private Equity',
    match: (t) => ['pe', 'private_equity', 'private equity'].includes(t?.toLowerCase()),
  },
  {
    key: 'venture_capital',
    label: 'Venture Capital',
    match: (t) => ['vc', 'venture_capital', 'venture capital'].includes(t?.toLowerCase()),
  },
  {
    key: 'private_credit',
    label: 'Private Credit',
    match: (t) => ['credit', 'private_credit', 'private credit'].includes(t?.toLowerCase()),
  },
  {
    key: 'hedge_fund',
    label: 'Hedge Funds',
    match: (t) => ['hedge', 'hedge_fund', 'hedge fund'].includes(t?.toLowerCase()),
  },
  {
    key: 'public_market',
    label: 'Public Market',
    match: (t) => ['etf', 'mutual_fund', 'public', '13f'].includes(t?.toLowerCase()),
  },
  { key: 'bdc', label: 'BDC', match: (t) => t?.toLowerCase() === 'bdc' },
  { key: 'other', label: 'Other', match: () => true },
]

function getFundTypeGroupKey(fund_type) {
  for (const g of FUND_TYPE_GROUPS) {
    if (g.match(fund_type)) return g.key
  }
  return 'other'
}

// Trend chart palette
const TREND_COLORS = [
  '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#84cc16',
]

// Sector → bar color for industry chart
const SECTOR_COLORS = {
  'Information Technology': '#6366f1',
  'Health Care': '#10b981',
  'Financials': '#3b82f6',
  'Consumer Discretionary': '#f59e0b',
  'Industrials': '#8b5cf6',
  'Communication Services': '#06b6d4',
  'Consumer Staples': '#84cc16',
  'Energy': '#ef4444',
  'Materials': '#f97316',
  'Real Estate': '#ec4899',
  'Utilities': '#14b8a6',
  'Unclassified': '#94a3b8',
}

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

  // Group by asset class (fund_type), preserving FUND_TYPE_GROUPS order
  const grouped = {}
  FUND_TYPE_GROUPS.forEach((g) => { grouped[g.key] = [] })
  funds.forEach((f) => { grouped[getFundTypeGroupKey(f.fund_type)].push(f) })

  // Group-level exposure totals
  const groupTotals = {}
  FUND_TYPE_GROUPS.forEach((g) => {
    groupTotals[g.key] = grouped[g.key].reduce(
      (sum, f) => sum + (f.total_exposure_usd ?? 0), 0,
    )
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
          {FUND_TYPE_GROUPS.flatMap((group) => {
            const groupFunds = grouped[group.key]
            if (!groupFunds?.length) return []
            const groupExposure = groupTotals[group.key]

            return [
              // Group header row
              <tr key={`hdr-${group.key}`}>
                <td
                  colSpan={6}
                  className="py-2 px-4 border-y border-secondary-100 bg-secondary-50"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-semibold text-secondary-700 uppercase tracking-wide">
                      {group.label}
                    </span>
                    {groupExposure > 0 && (
                      <span className="text-xs font-medium text-secondary-500">
                        {formatAUM(groupExposure)}
                      </span>
                    )}
                    <span className="text-xs text-secondary-400">
                      · {groupFunds.length} fund{groupFunds.length !== 1 ? 's' : ''}
                    </span>
                  </div>
                </td>
              </tr>,
              // Fund rows
              ...groupFunds.map((f) => (
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
// Industry Breakdown Chart
// ---------------------------------------------------------------------------

function IndustryBreakdownChart({ data, loading }) {
  if (loading) {
    return (
      <div className="space-y-2 py-2">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="h-7 bg-secondary-100 rounded animate-pulse" />
        ))}
      </div>
    )
  }

  const items = data?.industries ?? []
  if (!items.length) {
    return (
      <div className="h-32 flex items-center justify-center text-secondary-400 text-sm">
        No industry data available
      </div>
    )
  }

  const chartData = items
    .filter((d) => d.industry && d.industry !== 'Unclassified' && d.industry !== 'Unknown')
    .slice(0, 15)
    .map((d) => ({
      name: d.industry.length > 26 ? d.industry.slice(0, 26) + '…' : d.industry,
      value: d.value_usd,
      pct: d.pct,
      sector: d.sector,
      fill: SECTOR_COLORS[d.sector] ?? '#94a3b8',
    }))

  return (
    <>
      <ResponsiveContainer width="100%" height={400}>
        <BarChart
          data={chartData}
          layout="vertical"
          margin={{ left: 4, right: 48, top: 2, bottom: 4 }}
        >
          <CartesianGrid horizontal={false} stroke="#e2e8f0" />
          <XAxis
            type="number"
            tick={{ fontSize: 10, fill: '#94a3b8' }}
            axisLine={true}
            tickLine={true}
            tickFormatter={(v) => `$${(v / 1_000_000).toFixed(0)}M`}
          />
          <YAxis
            type="category"
            dataKey="name"
            width={160}
            tick={{ fontSize: 11, fill: '#64748b' }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            formatter={(value, _, props) => [
              `${formatAUM(value)} (${props.payload.pct?.toFixed(1)}%)`,
              props.payload.sector,
            ]}
            contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
          />
          <Bar dataKey="value" radius={[0, 3, 3, 0]}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.fill} fillOpacity={0.85} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <p className="text-xs text-secondary-400 mt-1 text-right">Showing classified holdings only</p>
    </>
  )
}

// ---------------------------------------------------------------------------
// Country Breakdown Chart
// ---------------------------------------------------------------------------

function CountryBreakdownChart({ data, loading }) {
  if (loading) {
    return (
      <div className="space-y-2 py-2">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="h-7 bg-secondary-100 rounded animate-pulse" />
        ))}
      </div>
    )
  }

  const items = (data?.countries ?? []).filter(
    (c) => c.country !== 'Unknown' && c.value_usd > 0,
  )
  if (items.length <= 2) {
    return (
      <div className="h-32 flex items-center justify-center text-secondary-400 text-sm">
        Insufficient country data
      </div>
    )
  }

  const chartData = items.map((d) => ({
    name: d.country.length > 20 ? d.country.slice(0, 20) + '…' : d.country,
    value: d.value_usd,
    pct: d.pct,
    holding_count: d.holding_count,
  }))

  return (
    <ResponsiveContainer width="100%" height={chartData.length * 34 + 32}>
      <BarChart
        data={chartData}
        layout="vertical"
        margin={{ left: 4, right: 48, top: 2, bottom: 4 }}
      >
        <CartesianGrid horizontal={false} stroke="#e2e8f0" />
        <XAxis
          type="number"
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          axisLine={true}
          tickLine={true}
          tickFormatter={(v) => `$${(v / 1_000_000).toFixed(0)}M`}
        />
        <YAxis
          type="category"
          dataKey="name"
          width={120}
          tick={{ fontSize: 11, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value, _, props) => [
            `${formatAUM(value)} (${props.payload.pct?.toFixed(1)}%)`,
            `${props.payload.holding_count} holdings`,
          ]}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="value" fill="#1d4ed8" fillOpacity={0.8} radius={[0, 3, 3, 0]} />
      </BarChart>
    </ResponsiveContainer>
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

  const { data: industryData, isLoading: industryLoading } = useQuery({
    queryKey: ['dashboard-industry-breakdown'],
    queryFn: () => fetchJSON('/api/dashboard/industry-breakdown'),
    staleTime: 10 * 60 * 1000,
  })

  const { data: countryData, isLoading: countryLoading } = useQuery({
    queryKey: ['dashboard-country-breakdown'],
    queryFn: () => fetchJSON('/api/dashboard/country-breakdown'),
    staleTime: 10 * 60 * 1000,
  })

  // Determine if geography data has enough known countries to be worth showing
  const { data: geoBreakdownData } = useQuery({
    queryKey: ['dashboard-geography-breakdown'],
    queryFn: () => fetchJSON('/api/dashboard/geography-breakdown'),
    staleTime: 10 * 60 * 1000,
  })
  const _geoArr = Array.isArray(geoBreakdownData)
    ? geoBreakdownData
    : (geoBreakdownData?.data ?? geoBreakdownData?.items ?? [])
  const _geoTotal = _geoArr.reduce((sum, g) => sum + (Number(g.value_usd) || 0), 0)
  const _geoKnown = _geoArr
    .filter((g) => g.country !== 'Unknown' && g.value_usd && !isNaN(Number(g.value_usd)) && Number(g.value_usd) > 0)
    .reduce((sum, g) => sum + (Number(g.value_usd) || 0), 0)
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

      {/* Industry + Country Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Top Industries</CardTitle>
            <p className="text-xs text-secondary-400 mt-0.5">
              By scaled exposure — latest quarter, color-coded by sector
            </p>
          </CardHeader>
          <CardContent>
            <IndustryBreakdownChart data={industryData} loading={industryLoading} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Geographic Exposure</CardTitle>
            <p className="text-xs text-secondary-400 mt-0.5">
              By country — latest quarter
            </p>
          </CardHeader>
          <CardContent>
            <CountryBreakdownChart data={countryData} loading={countryLoading} />
          </CardContent>
        </Card>
      </div>

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

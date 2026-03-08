import { useState, useEffect, useCallback } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  ArrowLeft,
  Briefcase,
  DollarSign,
  Building2,
  CalendarDays,
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  AlertCircle,
  Download,
  Loader2,
} from 'lucide-react'
import { downloadExport } from '../utils/exportUtils'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function formatAUM(value) {
  if (value === null || value === undefined) return '—'
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`
  return `$${value.toFixed(0)}`
}

function formatCurrency(value) {
  if (value === null || value === undefined) return '—'
  if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`
  return `$${value.toLocaleString()}`
}

const SECTOR_COLORS = {
  Financials: 'bg-blue-100 text-blue-800',
  Finance: 'bg-blue-100 text-blue-800',
  Healthcare: 'bg-green-100 text-green-800',
  'Health Care': 'bg-green-100 text-green-800',
  Technology: 'bg-purple-100 text-purple-800',
  'Information Technology': 'bg-purple-100 text-purple-800',
  Energy: 'bg-orange-100 text-orange-800',
  'Real Estate': 'bg-yellow-100 text-yellow-800',
  Industrials: 'bg-slate-100 text-slate-700',
  'Consumer Discretionary': 'bg-pink-100 text-pink-800',
  'Consumer Staples': 'bg-emerald-100 text-emerald-800',
  Materials: 'bg-lime-100 text-lime-800',
  Utilities: 'bg-cyan-100 text-cyan-800',
  'Communication Services': 'bg-indigo-100 text-indigo-800',
}

function sectorClasses(sector) {
  if (!sector) return 'bg-gray-100 text-gray-600'
  for (const [key, cls] of Object.entries(SECTOR_COLORS)) {
    if (sector.toLowerCase().includes(key.toLowerCase())) return cls
  }
  return 'bg-gray-100 text-gray-600'
}

function buildHoldingsUrl(fundId, filters) {
  const p = new URLSearchParams()
  p.set('page', String(filters.page))
  p.set('page_size', '50')
  p.set('fund_id', fundId)
  if (filters.search) p.set('search', filters.search)
  if (filters.hasValue) p.set('has_value', 'true')
  return `/api/holdings?${p.toString()}`
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ErrorBanner({ message }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <AlertCircle className="h-4 w-4 shrink-0" />
      <span>{message}</span>
    </div>
  )
}

function StatCard({ title, value, icon: Icon, description }) {
  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium text-secondary-500">{title}</p>
            <p className="text-2xl font-bold text-secondary-900 mt-1">{value ?? '—'}</p>
            {description && <p className="text-xs text-secondary-400 mt-1">{description}</p>}
          </div>
          <div className="h-12 w-12 rounded-full bg-primary-50 flex items-center justify-center">
            <Icon className="h-6 w-6 text-primary-600" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

const CHART_COLOR = '#1A4B9B'

function SectorChart({ sectors }) {
  const data = sectors.map((s) => ({
    name: s.sector.length > 20 ? s.sector.slice(0, 18) + '…' : s.sector,
    fullName: s.sector,
    value: s.holding_count,
    pct: s.percentage,
  }))

  return (
    <ResponsiveContainer width="100%" height={260}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 32, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
        <XAxis
          type="number"
          tick={{ fontSize: 11, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
          label={{
            value: 'Holdings',
            position: 'insideBottomRight',
            offset: -4,
            fontSize: 10,
            fill: '#94a3b8',
          }}
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
            const { pct, fullName } = props.payload
            return [`${value.toLocaleString()} holdings (${pct}%)`, fullName]
          }}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="value" fill={CHART_COLOR} radius={[0, 4, 4, 0]} maxBarSize={24} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Exposure Trend Chart (fund-scoped)
// ---------------------------------------------------------------------------

const TREND_COLORS = [
  '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#84cc16',
]

function ExposureTrendChart({ data, loading }) {
  if (loading) {
    return (
      <div className="h-64 flex items-center justify-center">
        <div className="space-y-3 w-full px-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="h-8 bg-secondary-200 rounded animate-pulse" />
          ))}
        </div>
      </div>
    )
  }

  if (!data?.dates?.length || !data?.series?.length) {
    return (
      <div className="h-64 flex items-center justify-center text-secondary-400 text-sm">
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
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={chartData} margin={{ left: 8, right: 16, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#64748b' }} axisLine={false} tickLine={false} />
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

function Pagination({ page, totalPages, onPageChange }) {
  if (totalPages <= 1) return null
  return (
    <div className="flex items-center justify-between px-4 py-3 border-t border-secondary-200">
      <span className="text-sm text-secondary-500">
        Page {page} of {totalPages.toLocaleString()}
      </span>
      <div className="flex items-center gap-1">
        <button
          onClick={() => onPageChange(1)}
          disabled={page === 1}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
        >
          <ChevronsLeft className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(page - 1)}
          disabled={page === 1}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(page + 1)}
          disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(totalPages)}
          disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
        >
          <ChevronsRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function FundDetailPage() {
  const { fund_id } = useParams()
  const [searchInput, setSearchInput] = useState('')
  const [filters, setFilters] = useState({ search: '', hasValue: false, page: 1 })
  const [isExporting, setIsExporting] = useState(false)
  const [trendDim, setTrendDim] = useState('sector')

  async function handleExport() {
    setIsExporting(true)
    try {
      const today = new Date().toISOString().split('T')[0]
      await downloadExport(
        `/api/funds/${fund_id}/export`,
        `lookthrough_fund_${fund_id}_${today}.csv`,
      )
    } finally {
      setIsExporting(false)
    }
  }

  // Debounce search 300ms
  useEffect(() => {
    const t = setTimeout(() => {
      setFilters((prev) => ({ ...prev, search: searchInput, page: 1 }))
    }, 300)
    return () => clearTimeout(t)
  }, [searchInput])

  const setFilter = useCallback((key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }))
  }, [])

  const clearFilters = useCallback(() => {
    setSearchInput('')
    setFilters({ search: '', hasValue: false, page: 1 })
  }, [])

  const hasActiveFilters = filters.search || filters.hasValue

  // Fund detail query
  const { data: fund, isLoading: fundLoading, error: fundError } = useQuery({
    queryKey: ['fund-detail', fund_id],
    queryFn: () => fetchJSON(`/api/funds/${fund_id}`),
    staleTime: 5 * 60 * 1000,
    retry: 1,
  })

  const { data: trendData, isLoading: trendLoading } = useQuery({
    queryKey: ['fund-exposure-trend', fund_id, trendDim],
    queryFn: () =>
      fetchJSON(
        `/api/dashboard/exposure-trend/fund/${fund_id}?dimension_type=${trendDim}&periods=8`,
      ),
    enabled: !!fund_id,
  })

  // Holdings for this fund
  const { data: holdingsData, isLoading: holdingsLoading } = useQuery({
    queryKey: ['fund-holdings', fund_id, filters.page, filters.search, filters.hasValue],
    queryFn: () => fetchJSON(buildHoldingsUrl(fund_id, filters)),
    placeholderData: (prev) => prev,
    staleTime: 2 * 60 * 1000,
  })

  const holdingsTotal = holdingsData?.total ?? 0
  const holdingsTotalPages = holdingsData?.total_pages ?? 1

  // ---- Loading skeleton ----
  if (fundLoading) {
    return (
      <div className="space-y-6">
        <div className="h-5 w-36 bg-secondary-200 rounded animate-pulse" />
        <div className="h-8 w-72 bg-secondary-200 rounded animate-pulse" />
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-28 bg-secondary-200 rounded-lg animate-pulse" />
          ))}
        </div>
      </div>
    )
  }

  // ---- Error / not found ----
  if (fundError || !fund) {
    return (
      <div className="space-y-4">
        <Link
          to="/dashboard"
          className="inline-flex items-center gap-1.5 text-sm text-secondary-500 hover:text-secondary-700 transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          Portfolio Overview
        </Link>
        <ErrorBanner
          message={
            fundError ? `Failed to load fund: ${fundError.message}` : 'Fund not found.'
          }
        />
      </div>
    )
  }

  const metaTags = [fund.fund_type, fund.strategy].filter(Boolean)

  return (
    <div className="space-y-6">

      {/* Back navigation */}
      <Link
        to="/dashboard"
        className="inline-flex items-center gap-1.5 text-sm text-secondary-500 hover:text-secondary-700 transition-colors"
      >
        <ArrowLeft className="h-4 w-4" />
        Portfolio Overview
      </Link>

      {/* Header */}
      <div className="flex items-start justify-between gap-4">
      <div className="flex flex-col gap-2">
        <h1 className="text-2xl font-bold text-secondary-900">{fund.fund_name}</h1>
        <div className="flex flex-wrap items-center gap-2">
          {fund.manager_name && (
            <span className="text-sm text-secondary-500">{fund.manager_name}</span>
          )}
          {fund.manager_name && metaTags.length > 0 && (
            <span className="text-secondary-300">·</span>
          )}
          {metaTags.map((tag) => (
            <span
              key={tag}
              className="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium bg-primary-50 text-primary-700 border border-primary-100"
            >
              {tag}
            </span>
          ))}
          {fund.vintage_year && (
            <span className="text-xs text-secondary-400">Vintage {fund.vintage_year}</span>
          )}
          {fund.base_currency && (
            <span className="text-xs text-secondary-400">{fund.base_currency}</span>
          )}
          {fund.source && (
            <span className="text-xs text-secondary-300 font-mono">{fund.source}</span>
          )}
        </div>
      </div>

        <button
          onClick={handleExport}
          disabled={isExporting}
          className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-secondary-700 border border-secondary-200 rounded-md bg-white hover:bg-secondary-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shrink-0"
        >
          {isExporting ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Download className="h-3.5 w-3.5" />
          )}
          Export CSV
        </button>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Total Holdings"
          value={fund.holding_count?.toLocaleString()}
          icon={Briefcase}
          description="Reported positions"
        />
        <StatCard
          title="Total AUM"
          value={formatAUM(fund.total_value)}
          icon={DollarSign}
          description="Sum of reported values"
        />
        <StatCard
          title="Unique Companies"
          value={fund.unique_companies?.toLocaleString()}
          icon={Building2}
          description="After entity resolution"
        />
        <StatCard
          title="As of Date"
          value={fund.as_of_date ?? '—'}
          icon={CalendarDays}
          description="Most recent report"
        />
      </div>

      {/* Two-column: sector chart + top holdings */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Sector breakdown */}
        <Card>
          <CardHeader>
            <CardTitle>Sector Allocation</CardTitle>
          </CardHeader>
          <CardContent>
            {fund.sector_breakdown?.length ? (
              <SectorChart sectors={fund.sector_breakdown} />
            ) : (
              <div className="h-64 flex items-center justify-center text-secondary-400 text-sm">
                No sector data available
              </div>
            )}
          </CardContent>
        </Card>

        {/* Top 10 holdings by value */}
        <Card>
          <CardHeader>
            <CardTitle>Top 10 Holdings by Value</CardTitle>
          </CardHeader>
          <CardContent>
            {!fund.top_holdings?.length ? (
              <div className="py-8 text-center text-secondary-400 text-sm">
                Value data not available for this fund
              </div>
            ) : (
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-secondary-200">
                      <th className="text-left py-2 px-1 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        Company
                      </th>
                      <th className="text-right py-2 px-1 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        Value
                      </th>
                      <th className="text-right py-2 px-1 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        % NAV
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {fund.top_holdings.map((h, i) => (
                      <tr key={i} className="border-b border-secondary-100 hover:bg-secondary-50">
                        <td className="py-2 px-1 max-w-[160px] truncate" title={h.company_name}>
                          {h.company_id ? (
                            <Link
                              to={`/companies/${h.company_id}`}
                              className="text-primary-700 hover:text-primary-900 hover:underline"
                            >
                              {h.company_name ?? '—'}
                            </Link>
                          ) : (
                            <span className="text-secondary-800">{h.company_name ?? '—'}</span>
                          )}
                        </td>
                        <td className="py-2 px-1 text-right tabular-nums font-medium text-secondary-800">
                          {h.reported_value_usd != null
                            ? formatCurrency(h.reported_value_usd)
                            : '—'}
                        </td>
                        <td className="py-2 px-1 text-right tabular-nums text-secondary-500">
                          {h.pct_nav != null ? `${h.pct_nav.toFixed(2)}%` : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Exposure Trend */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Exposure Trend</CardTitle>
            <div className="flex items-center gap-1">
              {['sector', 'geography'].map((dim) => (
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

      {/* Full holdings section */}
      <div>
        <h2 className="text-lg font-semibold text-secondary-900 mb-3">All Holdings</h2>

        {/* Filter bar */}
        <Card className="mb-4">
          <CardContent className="p-4">
            <div className="flex flex-wrap items-center gap-3">
              <div className="relative flex-1 min-w-[200px]">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-secondary-400" />
                <input
                  type="text"
                  placeholder="Search company name…"
                  value={searchInput}
                  onChange={(e) => setSearchInput(e.target.value)}
                  className="w-full pl-9 pr-3 py-2 text-sm border border-secondary-200 rounded-md bg-white text-secondary-900 placeholder-secondary-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                />
              </div>

              <label className="flex items-center gap-2 text-sm text-secondary-700 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={filters.hasValue}
                  onChange={(e) => setFilter('hasValue', e.target.checked)}
                  className="h-4 w-4 rounded border-secondary-300 text-primary-600 focus:ring-primary-500"
                />
                With Value Only
              </label>

              {hasActiveFilters && (
                <button
                  onClick={clearFilters}
                  className="flex items-center gap-1.5 px-3 py-2 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
                >
                  <X className="h-3.5 w-3.5" />
                  Clear
                </button>
              )}

              <span className="ml-auto text-sm text-secondary-500 whitespace-nowrap">
                {holdingsTotal.toLocaleString()} holdings
              </span>
            </div>
          </CardContent>
        </Card>

        {/* Holdings table */}
        <Card className="overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-secondary-200 bg-secondary-50">
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Company
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Sector
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Country
                  </th>
                  <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Reported Value
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Date
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Source
                  </th>
                </tr>
              </thead>
              <tbody>
                {holdingsLoading ? (
                  [...Array(10)].map((_, i) => (
                    <tr key={i} className="border-b border-secondary-100">
                      <td className="py-3 px-4">
                        <div className="h-3.5 w-40 bg-secondary-200 rounded animate-pulse" />
                      </td>
                      <td className="py-3 px-4">
                        <div className="h-5 w-24 bg-secondary-100 rounded-full animate-pulse" />
                      </td>
                      <td className="py-3 px-4">
                        <div className="h-3 w-16 bg-secondary-100 rounded animate-pulse" />
                      </td>
                      <td className="py-3 px-4 text-right">
                        <div className="h-3 w-20 bg-secondary-100 rounded animate-pulse ml-auto" />
                      </td>
                      <td className="py-3 px-4">
                        <div className="h-3 w-20 bg-secondary-100 rounded animate-pulse" />
                      </td>
                      <td className="py-3 px-4">
                        <div className="h-3 w-16 bg-secondary-100 rounded animate-pulse" />
                      </td>
                    </tr>
                  ))
                ) : !holdingsData?.items?.length ? (
                  <tr>
                    <td colSpan={6} className="py-16 text-center">
                      <p className="text-secondary-400 text-sm">No holdings match your filters.</p>
                    </td>
                  </tr>
                ) : (
                  holdingsData.items.map((row) => (
                    <tr
                      key={row.holding_id}
                      className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors"
                    >
                      <td className="py-3 px-4 max-w-[200px]">
                        {row.company_id ? (
                          <Link
                            to={`/companies/${row.company_id}`}
                            className="font-semibold text-primary-700 hover:text-primary-900 hover:underline truncate block"
                            title={row.company_name ?? '—'}
                          >
                            {row.company_name ?? '—'}
                          </Link>
                        ) : (
                          <span className="font-semibold text-primary-700 truncate block" title={row.company_name ?? '—'}>
                            {row.company_name ?? '—'}
                          </span>
                        )}
                      </td>
                      <td className="py-3 px-4">
                        {row.sector ? (
                          <span
                            className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${sectorClasses(row.sector)}`}
                          >
                            {row.sector}
                          </span>
                        ) : (
                          <span className="text-secondary-300 text-xs">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-secondary-600">
                        {row.country ?? <span className="text-secondary-300">—</span>}
                      </td>
                      <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                        {row.reported_value != null ? (
                          formatCurrency(row.reported_value)
                        ) : (
                          <span className="text-secondary-300">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-secondary-500 text-xs">
                        {row.date_reported ?? '—'}
                      </td>
                      <td className="py-3 px-4">
                        <span className="text-xs text-secondary-400">{row.source ?? '—'}</span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          {!holdingsLoading && holdingsData && (
            <Pagination
              page={filters.page}
              totalPages={holdingsTotalPages}
              onPageChange={(p) => setFilters((prev) => ({ ...prev, page: p }))}
            />
          )}
        </Card>
      </div>
    </div>
  )
}

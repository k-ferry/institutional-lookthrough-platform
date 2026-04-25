import { useState, useEffect, useCallback } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
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
  ArrowLeft,
  Briefcase,
  DollarSign,
  BarChart2,
  CalendarDays,
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  AlertCircle,
  Info,
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

// Fund type → badge classes
const FUND_TYPE_CLASSES = {
  PE: 'bg-blue-900 text-white',
  VC: 'bg-blue-600 text-white',
  hedge: 'bg-purple-700 text-white',
  credit: 'bg-teal-600 text-white',
  BDC: 'bg-emerald-600 text-white',
  ETF: 'bg-sky-200 text-sky-900',
  synthetic: 'bg-gray-500 text-white',
}

// Source → verbose description (subtle metadata only)
const SOURCE_VERBOSE = {
  pdf_document: 'Private fund documents (PDF)',
  '13f_filing':  'SEC Form 13F',
  bdc_filing:    'BDC 10-K filing',
  synthetic:     'Synthetic data',
}

function fundTypeBadgeClasses(type) {
  if (!type) return 'bg-gray-200 text-gray-700'
  for (const [key, cls] of Object.entries(FUND_TYPE_CLASSES)) {
    if (type.toLowerCase() === key.toLowerCase()) return cls
  }
  return 'bg-gray-200 text-gray-700'
}

// Sector → chip classes (for small inline chips)
const SECTOR_CHIP_COLORS = {
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
  Unclassified: 'bg-gray-100 text-gray-500',
}

function sectorChipClasses(sector) {
  if (!sector || sector === 'Unclassified') return 'bg-gray-100 text-gray-500'
  for (const [key, cls] of Object.entries(SECTOR_CHIP_COLORS)) {
    if (sector.toLowerCase().includes(key.toLowerCase())) return cls
  }
  return 'bg-gray-100 text-gray-500'
}

// Sector → chart hex color (for bar/area charts)
const SECTOR_CHART_COLORS = {
  Financials: '#3b82f6',
  Finance: '#3b82f6',
  'Information Technology': '#8b5cf6',
  Technology: '#8b5cf6',
  'Health Care': '#10b981',
  Healthcare: '#10b981',
  Energy: '#f59e0b',
  Industrials: '#64748b',
  'Consumer Discretionary': '#ec4899',
  'Consumer Staples': '#059669',
  Materials: '#84cc16',
  Utilities: '#06b6d4',
  'Communication Services': '#6366f1',
  'Real Estate': '#eab308',
  Unclassified: '#9ca3af',
}

function sectorChartColor(sector) {
  if (!sector) return '#9ca3af'
  for (const [key, color] of Object.entries(SECTOR_CHART_COLORS)) {
    if (sector.toLowerCase().includes(key.toLowerCase())) return color
  }
  return '#94a3b8'
}

const TREND_COLORS = [
  '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#84cc16',
]

function buildFundHoldingsUrl(fundId, filters) {
  const p = new URLSearchParams()
  p.set('page', String(filters.page))
  p.set('page_size', '50')
  if (filters.search) p.set('search', filters.search)
  if (filters.sortBy) p.set('sort_by', filters.sortBy)
  if (filters.sortDir) p.set('sort_dir', filters.sortDir)
  return `/api/funds/${fundId}/holdings?${p.toString()}`
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
          <div className="flex-1 min-w-0">
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

// Horizontal bar chart for sector or geography (by value_usd)
function ValueBarChart({ data, height, colorFn }) {
  if (!data?.length) {
    return (
      <div className="flex items-center justify-center text-secondary-400 text-sm" style={{ height }}>
        No data available
      </div>
    )
  }

  const chartData = data.map((d) => ({
    name: d.name.length > 24 ? d.name.slice(0, 22) + '…' : d.name,
    fullName: d.name,
    value: d.value,
    pct: d.pct,
    sector: d.sector,
  }))

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 52, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
        <XAxis
          type="number"
          tickFormatter={(v) => formatAUM(v)}
          tick={{ fontSize: 10, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          type="category"
          dataKey="name"
          width={148}
          tick={{ fontSize: 11, fill: '#475569' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value, _name, props) => {
            const { pct, fullName, sector } = props.payload
            const label = sector && sector !== fullName ? `${fullName} · ${sector}` : fullName
            return [`${formatCurrency(value)} (${pct}%)`, label]
          }}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} maxBarSize={26}>
          {chartData.map((entry, index) => (
            <Cell key={index} fill={colorFn ? colorFn(entry) : '#1A4B9B'} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

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
  const [filters, setFilters] = useState({ search: '', page: 1, sortBy: 'reported_value_usd', sortDir: 'desc' })
  const [isExporting, setIsExporting] = useState(false)

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

  // Debounce search 300 ms
  useEffect(() => {
    const t = setTimeout(() => {
      setFilters((prev) => ({ ...prev, search: searchInput, page: 1 }))
    }, 300)
    return () => clearTimeout(t)
  }, [searchInput])

  const clearSearch = useCallback(() => {
    setSearchInput('')
    setFilters((prev) => ({ ...prev, search: '', page: 1 }))
  }, [])

  // Fund detail
  const { data: fund, isLoading: fundLoading, error: fundError } = useQuery({
    queryKey: ['fund-detail', fund_id],
    queryFn: () => fetchJSON(`/api/funds/${fund_id}`),
    staleTime: 5 * 60 * 1000,
    retry: 1,
  })

  // Exposure trend — only fetch if fund loaded and has multiple quarters
  const { data: trendData, isLoading: trendLoading } = useQuery({
    queryKey: ['fund-exposure-trend', fund_id],
    queryFn: () => fetchJSON(`/api/funds/${fund_id}/exposure-trend?dimension_type=sector&periods=8`),
    enabled: !!fund_id && (fund?.quarter_count ?? 0) > 1,
    staleTime: 5 * 60 * 1000,
  })

  // Paginated holdings for latest quarter
  const { data: holdingsData, isLoading: holdingsLoading } = useQuery({
    queryKey: ['fund-holdings', fund_id, filters.page, filters.search, filters.sortBy, filters.sortDir],
    queryFn: () => fetchJSON(buildFundHoldingsUrl(fund_id, filters)),
    placeholderData: (prev) => prev,
    staleTime: 2 * 60 * 1000,
    enabled: !!fund_id,
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
          message={fundError ? `Failed to load fund: ${fundError.message}` : 'Fund not found.'}
        />
      </div>
    )
  }

  const isSingleQuarter = (fund.quarter_count ?? 0) <= 1
  const sectorCount = fund.sector_breakdown?.length ?? 0

  const _geoBreakdown = fund.geography_breakdown ?? []
  const _geoTotal = _geoBreakdown.reduce((sum, g) => sum + (g.value_usd ?? 0), 0)
  const _geoKnown = _geoBreakdown
    .filter((g) => g.country !== 'Unknown')
    .reduce((sum, g) => sum + (g.value_usd ?? 0), 0)
  const hasGeography = _geoTotal > 0 && _geoKnown / _geoTotal > 0.20

  // Prepare chart data
  const sectorChartData = (fund.sector_breakdown ?? []).map((s) => ({
    name: s.sector,
    value: s.value_usd,
    pct: s.pct,
  }))

  const industryChartData = (fund.industry_breakdown ?? []).map((ind) => ({
    name: ind.industry,
    value: ind.value_usd,
    pct: ind.pct,
    sector: ind.sector,
  }))

  const geoChartData = (fund.geography_breakdown ?? [])
    .filter((g) => g.value_usd > 0)
    .map((g) => ({
      name: g.country,
      value: g.value_usd,
      pct: g.pct,
    }))

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
        <div className="flex flex-col gap-2 min-w-0">
          <h1 className="text-2xl font-bold text-secondary-900 truncate">{fund.fund_name}</h1>

          {/* Badges + meta row */}
          <div className="flex flex-wrap items-center gap-2">
            {fund.fund_type && (
              <span
                className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${fundTypeBadgeClasses(fund.fund_type)}`}
              >
                {fund.fund_type}
              </span>
            )}
            {fund.manager_name && (
              <>
                <span className="text-secondary-300">·</span>
                <span className="text-sm text-secondary-500">{fund.manager_name}</span>
              </>
            )}
            {fund.strategy && (
              <>
                <span className="text-secondary-300">·</span>
                <span className="text-sm text-secondary-400">{fund.strategy}</span>
              </>
            )}
            {fund.latest_as_of_date && (
              <>
                <span className="text-secondary-300">·</span>
                <span className="text-xs text-secondary-400 flex items-center gap-1">
                  <CalendarDays className="h-3 w-3" />
                  {fund.latest_as_of_date}
                </span>
              </>
            )}
          </div>
          {fund.source && (
            <p style={{ fontSize: '11px' }} className="text-secondary-400 mt-0.5">
              Data: {SOURCE_VERBOSE[fund.source] ?? fund.source}
            </p>
          )}
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
          title="Total Exposure"
          value={formatAUM(fund.total_exposure_usd)}
          icon={DollarSign}
          description="Sum of reported values"
        />
        <StatCard
          title="Holdings"
          value={fund.holding_count?.toLocaleString()}
          icon={Briefcase}
          description="Latest quarter"
        />
        <StatCard
          title="Sectors"
          value={sectorCount.toLocaleString()}
          icon={BarChart2}
          description="Distinct classifications"
        />
        <StatCard
          title="Quarters of Data"
          value={fund.quarter_count?.toLocaleString() ?? '—'}
          icon={CalendarDays}
          description={isSingleQuarter ? 'Point-in-time snapshot' : 'Historical periods available'}
        />
      </div>

      {/* Two-column: sector chart + top 15 holdings */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">

        {/* Sector allocation — 55% width */}
        <Card className="lg:col-span-3">
          <CardHeader>
            <CardTitle>Sector Allocation</CardTitle>
          </CardHeader>
          <CardContent>
            <ValueBarChart
              data={sectorChartData}
              height={Math.max(220, sectorChartData.length * 36 + 40)}
              colorFn={(entry) => sectorChartColor(entry.fullName ?? entry.name)}
            />
          </CardContent>
        </Card>

        {/* Top 15 holdings — 45% width */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Top 15 Holdings</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {!fund.top_holdings?.length ? (
              <div className="py-8 text-center text-secondary-400 text-sm px-4">
                Value data not available for this fund
              </div>
            ) : (
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-secondary-200 bg-secondary-50">
                      <th className="text-left py-2 px-3 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        Company
                      </th>
                      <th className="text-right py-2 px-3 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        Value
                      </th>
                      <th className="text-right py-2 px-3 font-medium text-secondary-500 text-xs uppercase tracking-wide">
                        %
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {fund.top_holdings.map((h, i) => (
                      <tr key={i} className="border-b border-secondary-100 hover:bg-secondary-50">
                        <td className="py-2 px-3">
                          <div className="flex flex-col gap-0.5">
                            {h.company_id ? (
                              <Link
                                to={`/companies/${h.company_id}`}
                                className="text-xs font-medium text-primary-700 hover:underline truncate max-w-[140px] block"
                                title={h.company_name}
                              >
                                {h.company_name ?? '—'}
                              </Link>
                            ) : (
                              <span className="text-xs font-medium text-secondary-800 truncate max-w-[140px] block" title={h.company_name}>
                                {h.company_name ?? '—'}
                              </span>
                            )}
                            <span
                              className={`inline-flex items-center self-start rounded-full px-1.5 py-0 text-xs font-medium ${sectorChipClasses(h.sector)}`}
                              style={{ fontSize: '10px' }}
                            >
                              {h.sector}
                            </span>
                          </div>
                        </td>
                        <td className="py-2 px-3 text-right tabular-nums font-medium text-secondary-800 text-xs">
                          {h.reported_value_usd != null ? formatCurrency(h.reported_value_usd) : '—'}
                        </td>
                        <td className="py-2 px-3 text-right tabular-nums text-secondary-500 text-xs">
                          {h.pct_of_fund != null ? `${h.pct_of_fund.toFixed(1)}%` : '—'}
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

      {/* Industry breakdown */}
      {fund.industry_breakdown?.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Top Industries by Exposure</CardTitle>
          </CardHeader>
          <CardContent>
            <ValueBarChart
              data={industryChartData}
              height={Math.max(220, industryChartData.length * 32 + 40)}
              colorFn={(entry) => sectorChartColor(entry.sector)}
            />
          </CardContent>
        </Card>
      )}

      {/* Exposure trend or BDC banner */}
      {isSingleQuarter ? (
        <div className="flex items-start gap-3 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3.5 text-sm text-blue-800">
          <Info className="h-4 w-4 shrink-0 mt-0.5" />
          <span>
            <strong>Point-in-time data</strong> — this fund represents a single{' '}
            {fund.source === 'bdc_filing' ? 'BDC 10-K filing' : 'filing'} snapshot. Exposure trend
            charts require multiple reporting periods.
          </span>
        </div>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Sector Exposure Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ExposureTrendChart data={trendData} loading={trendLoading} />
          </CardContent>
        </Card>
      )}

      {/* Geography exposure */}
      {hasGeography && (
        <Card>
          <CardHeader>
            <CardTitle>Geography Exposure</CardTitle>
          </CardHeader>
          <CardContent>
            <ValueBarChart
              data={geoChartData}
              height={Math.max(160, geoChartData.length * 32 + 40)}
              colorFn={null}
            />
          </CardContent>
        </Card>
      )}

      {/* Full holdings table */}
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

              {filters.search && (
                <button
                  onClick={clearSearch}
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
                    Industry
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Country
                  </th>
                  <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    Value
                  </th>
                  <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    % Fund
                  </th>
                  <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                    As of Date
                  </th>
                </tr>
              </thead>
              <tbody>
                {holdingsLoading ? (
                  [...Array(10)].map((_, i) => (
                    <tr key={i} className="border-b border-secondary-100">
                      {[...Array(7)].map((_, j) => (
                        <td key={j} className="py-3 px-4">
                          <div className="h-3.5 bg-secondary-200 rounded animate-pulse" style={{ width: `${60 + j * 10}%` }} />
                        </td>
                      ))}
                    </tr>
                  ))
                ) : !holdingsData?.items?.length ? (
                  <tr>
                    <td colSpan={7} className="py-16 text-center">
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
                          <span
                            className="font-semibold text-secondary-800 truncate block"
                            title={row.company_name ?? '—'}
                          >
                            {row.company_name ?? '—'}
                          </span>
                        )}
                      </td>
                      <td className="py-3 px-4">
                        <span
                          className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${sectorChipClasses(row.sector)}`}
                        >
                          {row.sector}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-secondary-600 max-w-[160px] truncate" title={row.industry}>
                        {row.industry !== 'Unclassified' ? row.industry : (
                          <span className="text-secondary-300">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-secondary-600">
                        {row.country !== 'Unknown' ? row.country : (
                          <span className="text-secondary-300">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                        {row.reported_value_usd != null ? (
                          formatCurrency(row.reported_value_usd)
                        ) : (
                          <span className="text-secondary-300">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-right tabular-nums text-secondary-500">
                        {row.pct_of_fund != null ? `${row.pct_of_fund.toFixed(2)}%` : (
                          <span className="text-secondary-300">—</span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-secondary-500 text-xs">
                        {row.as_of_date ?? '—'}
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

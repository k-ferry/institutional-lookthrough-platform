import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Search, TrendingUp } from 'lucide-react'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'

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

function formatDate(d) {
  if (!d) return '—'
  return new Date(d).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
}

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

// ---------------------------------------------------------------------------
// Fund type display
// ---------------------------------------------------------------------------

const FUND_TYPE_META = {
  pe:        { label: 'Private Equity',  short: 'PE',      badgeClasses: 'bg-blue-900 text-white',      color: '#1e3a8a' },
  vc:        { label: 'Venture Capital', short: 'VC',      badgeClasses: 'bg-blue-600 text-white',      color: '#2563eb' },
  hedge:     { label: 'Hedge Fund',      short: 'Hedge',   badgeClasses: 'bg-purple-700 text-white',    color: '#7e22ce' },
  credit:    { label: 'Credit',          short: 'Credit',  badgeClasses: 'bg-teal-600 text-white',      color: '#0d9488' },
  bdc:       { label: 'BDC',             short: 'BDC',     badgeClasses: 'bg-emerald-600 text-white',   color: '#059669' },
  etf:       { label: 'ETF / Mutual',    short: 'ETF',     badgeClasses: 'bg-sky-200 text-sky-900',     color: '#38bdf8' },
  synthetic: { label: 'Synthetic',       short: 'Synthetic', badgeClasses: 'bg-gray-400 text-white',   color: '#9ca3af' },
}

function fundTypeMeta(type) {
  if (!type) return { label: type ?? '—', short: '—', badgeClasses: 'bg-gray-200 text-gray-600', color: '#d1d5db' }
  return FUND_TYPE_META[type.toLowerCase()] ?? { label: type, short: type, badgeClasses: 'bg-gray-200 text-gray-600', color: '#d1d5db' }
}

// Sector chip colors
const SECTOR_CHIP = {
  Financials: 'bg-blue-100 text-blue-800',
  Technology: 'bg-purple-100 text-purple-800',
  'Information Technology': 'bg-purple-100 text-purple-800',
  Healthcare: 'bg-green-100 text-green-800',
  'Health Care': 'bg-green-100 text-green-800',
  Energy: 'bg-orange-100 text-orange-800',
  Industrials: 'bg-slate-100 text-slate-700',
  'Consumer Discretionary': 'bg-pink-100 text-pink-800',
  'Consumer Staples': 'bg-emerald-100 text-emerald-800',
  Materials: 'bg-lime-100 text-lime-800',
  Utilities: 'bg-cyan-100 text-cyan-800',
  'Communication Services': 'bg-indigo-100 text-indigo-800',
  'Real Estate': 'bg-yellow-100 text-yellow-800',
}

function sectorChipClasses(sector) {
  if (!sector) return 'bg-gray-100 text-gray-500'
  for (const [key, cls] of Object.entries(SECTOR_CHIP)) {
    if (sector.toLowerCase().includes(key.toLowerCase())) return cls
  }
  return 'bg-gray-100 text-gray-500'
}

// ---------------------------------------------------------------------------
// Filter pills
// ---------------------------------------------------------------------------

const TYPE_FILTER_PILLS = [
  { key: 'all',    label: 'All Funds',       types: null },
  { key: 'pe',     label: 'Private Equity',  types: ['pe'] },
  { key: 'vc',     label: 'Venture Capital', types: ['vc'] },
  { key: 'credit', label: 'Credit',          types: ['credit'] },
  { key: 'hedge',  label: 'Hedge Fund',      types: ['hedge'] },
  { key: 'etf',    label: 'ETF & Mutual',    types: ['etf'] },
  { key: 'bdc',    label: 'BDC',             types: ['bdc'] },
]

// ---------------------------------------------------------------------------
// Donut chart
// ---------------------------------------------------------------------------

const DONUT_ROUNDING_LABEL = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, name }) => {
  if (percent < 0.04) return null
  const RADIAN = Math.PI / 180
  const radius = innerRadius + (outerRadius - innerRadius) * 0.55
  const x = cx + radius * Math.cos(-midAngle * RADIAN)
  const y = cy + radius * Math.sin(-midAngle * RADIAN)
  return (
    <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" fontSize={11} fontWeight={600}>
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  )
}

function AllocationDonut({ byType, loading }) {
  if (loading) {
    return <div className="h-64 flex items-center justify-center"><div className="h-40 w-40 rounded-full bg-secondary-100 animate-pulse" /></div>
  }
  if (!byType?.length) return null

  return (
    <ResponsiveContainer width="100%" height={260}>
      <PieChart>
        <Pie
          data={byType}
          dataKey="total_exposure_usd"
          nameKey="fund_type"
          cx="50%"
          cy="50%"
          innerRadius={60}
          outerRadius={100}
          labelLine={false}
          label={DONUT_ROUNDING_LABEL}
        >
          {byType.map((entry) => (
            <Cell key={entry.fund_type} fill={fundTypeMeta(entry.fund_type).color} />
          ))}
        </Pie>
        <Tooltip
          formatter={(value, _name, props) => [
            `${formatAUM(value)} (${props.payload.pct_of_portfolio?.toFixed(1)}%)`,
            fundTypeMeta(props.payload.fund_type).label,
          ]}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
      </PieChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Fund bar chart
// ---------------------------------------------------------------------------

function FundAllocationBar({ funds, loading }) {
  if (loading) {
    return (
      <div className="space-y-2 p-4">
        {[...Array(6)].map((_, i) => (
          <div key={i} className="h-6 bg-secondary-100 rounded animate-pulse" style={{ width: `${40 + i * 10}%` }} />
        ))}
      </div>
    )
  }

  const top = (funds ?? []).slice(0, 15).map((f) => ({
    name: f.fund_name.length > 22 ? f.fund_name.slice(0, 20) + '…' : f.fund_name,
    fullName: f.fund_name,
    value: f.total_exposure_usd ?? 0,
    pct: f.pct_of_portfolio ?? 0,
    fund_type: f.fund_type,
    fund_id: f.fund_id,
  }))

  return (
    <ResponsiveContainer width="100%" height={Math.max(240, top.length * 34 + 24)}>
      <BarChart data={top} layout="vertical" margin={{ left: 8, right: 56, top: 4, bottom: 4 }}>
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
          width={140}
          tick={{ fontSize: 11, fill: '#475569' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value, _name, props) => [
            `${formatAUM(value)} · ${props.payload.pct}% of portfolio`,
            props.payload.fullName,
          ]}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} maxBarSize={22}>
          {top.map((entry) => (
            <Cell key={entry.fund_id} fill={fundTypeMeta(entry.fund_type).color} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Fund card
// ---------------------------------------------------------------------------

function FundCard({ fund }) {
  const meta = fundTypeMeta(fund.fund_type)
  const pct = fund.pct_of_portfolio ?? 0

  return (
    <Link
      to={`/funds/${fund.fund_id}`}
      className="block rounded-lg border border-secondary-200 bg-white p-5 hover:shadow-md hover:border-primary-300 transition-all group"
    >
      <div className="flex items-start justify-between gap-2 mb-3">
        <h3 className="font-semibold text-secondary-900 text-sm leading-snug group-hover:text-primary-700 line-clamp-2">
          {fund.fund_name}
        </h3>
        <span className={`shrink-0 inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold ${meta.badgeClasses}`}>
          {meta.short}
        </span>
      </div>

      {/* Exposure + % */}
      <div className="mb-3">
        <p className="text-2xl font-bold text-secondary-900 tabular-nums leading-tight">
          {formatAUM(fund.total_exposure_usd)}
        </p>
        <p className="text-xs text-secondary-500 mt-0.5">{pct.toFixed(1)}% of portfolio</p>
      </div>

      {/* Mini progress bar */}
      <div className="h-1.5 rounded-full bg-secondary-100 mb-3 overflow-hidden">
        <div
          className="h-full rounded-full"
          style={{ width: `${Math.min(pct * 2, 100)}%`, backgroundColor: meta.color }}
        />
      </div>

      {/* Holdings + top sectors */}
      <div className="flex items-center justify-between text-xs text-secondary-500 mb-2">
        <span>{(fund.holding_count ?? 0).toLocaleString()} holdings</span>
        <span>{formatDate(fund.latest_as_of_date)}</span>
      </div>

      {fund.top_sectors?.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-2">
          {fund.top_sectors.slice(0, 3).map((s) => (
            <span
              key={s}
              className={`inline-flex items-center px-1.5 py-0 rounded-full text-xs font-medium ${sectorChipClasses(s)}`}
              style={{ fontSize: '10px' }}
            >
              {s}
            </span>
          ))}
        </div>
      )}
    </Link>
  )
}

function FundCardSkeleton() {
  return (
    <div className="rounded-lg border border-secondary-200 bg-white p-5 space-y-3 animate-pulse">
      <div className="flex justify-between gap-2">
        <div className="h-4 w-36 bg-secondary-200 rounded" />
        <div className="h-5 w-10 bg-secondary-100 rounded" />
      </div>
      <div className="h-7 w-24 bg-secondary-200 rounded" />
      <div className="h-1.5 w-full bg-secondary-100 rounded-full" />
      <div className="flex justify-between">
        <div className="h-3 w-20 bg-secondary-100 rounded" />
        <div className="h-3 w-16 bg-secondary-100 rounded" />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Legend
// ---------------------------------------------------------------------------

function TypeLegend({ byType }) {
  if (!byType?.length) return null
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1.5 mt-4">
      {byType.map((t) => {
        const meta = fundTypeMeta(t.fund_type)
        return (
          <div key={t.fund_type} className="flex items-center gap-1.5">
            <div className="h-2.5 w-2.5 rounded-full shrink-0" style={{ backgroundColor: meta.color }} />
            <span className="text-xs text-secondary-600">{meta.label}</span>
            <span className="text-xs text-secondary-400">{t.pct_of_portfolio?.toFixed(1)}%</span>
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function FundsPage() {
  const [search, setSearch] = useState('')
  const [typeFilter, setTypeFilter] = useState('all')

  const { data, isLoading, isError } = useQuery({
    queryKey: ['funds-allocation'],
    queryFn: () => fetchJSON('/api/funds/allocation'),
    staleTime: 5 * 60 * 1000,
  })

  const totalExposure = data?.total_portfolio_exposure ?? 0
  const byType = data?.by_type ?? []
  const allFunds = data?.by_fund ?? []

  // Fund type filter
  const filtered = useMemo(() => {
    const pill = TYPE_FILTER_PILLS.find((p) => p.key === typeFilter)
    const allowedTypes = pill?.types ?? null
    const q = search.trim().toLowerCase()

    return allFunds.filter((f) => {
      if (allowedTypes && !allowedTypes.includes((f.fund_type ?? '').toLowerCase())) return false
      if (q && !f.fund_name.toLowerCase().includes(q)) return false
      return true
    })
  }, [allFunds, typeFilter, search])

  return (
    <div className="space-y-6">

      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Portfolio — Fund Exposure Overview</h1>
        <p className="text-4xl font-bold text-primary-800 mt-2 tabular-nums tracking-tight">
          {isLoading ? <span className="h-10 w-48 bg-secondary-200 rounded animate-pulse inline-block" /> : formatAUM(totalExposure)}
        </p>
        <p className="text-sm text-secondary-500 mt-1">Total portfolio exposure across {allFunds.length} funds</p>
      </div>

      {isError && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          Failed to load allocation data. Please try refreshing.
        </div>
      )}

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">

        {/* Donut — allocation by type */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Allocation by Fund Type</CardTitle>
          </CardHeader>
          <CardContent>
            <AllocationDonut byType={byType} loading={isLoading} />
            <TypeLegend byType={byType} />
          </CardContent>
        </Card>

        {/* Horizontal bar — allocation by fund */}
        <Card className="lg:col-span-3">
          <CardHeader>
            <CardTitle>Allocation by Fund</CardTitle>
          </CardHeader>
          <CardContent className="overflow-hidden">
            <FundAllocationBar funds={allFunds} loading={isLoading} />
          </CardContent>
        </Card>
      </div>

      {/* Filter row */}
      <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">

        {/* Type pills */}
        <div className="flex flex-wrap gap-2">
          {TYPE_FILTER_PILLS.map((pill) => {
            const count = pill.types
              ? allFunds.filter((f) => pill.types.includes((f.fund_type ?? '').toLowerCase())).length
              : allFunds.length
            if (count === 0 && pill.key !== 'all') return null
            const isActive = typeFilter === pill.key
            return (
              <button
                key={pill.key}
                onClick={() => setTypeFilter(pill.key)}
                className={`px-3 py-1.5 rounded-full text-sm font-medium border transition-colors ${
                  isActive
                    ? 'bg-primary-700 text-white border-primary-700'
                    : 'bg-white text-secondary-600 border-secondary-200 hover:border-primary-400 hover:text-primary-700'
                }`}
              >
                {pill.label}
                <span className={`ml-1.5 text-xs ${isActive ? 'text-primary-200' : 'text-secondary-400'}`}>
                  {count}
                </span>
              </button>
            )
          })}
        </div>

        {/* Search */}
        <div className="relative sm:ml-auto w-full sm:w-64">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-secondary-400" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search funds…"
            className="w-full pl-9 pr-3 py-2 text-sm border border-secondary-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-300 focus:border-primary-400 bg-white"
          />
        </div>
      </div>

      {/* Fund cards */}
      {!isLoading && filtered.length === 0 ? (
        <div className="rounded-lg border border-secondary-200 bg-white p-12 text-center text-secondary-500">
          No funds match your current filters.
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {isLoading
            ? [...Array(8)].map((_, i) => <FundCardSkeleton key={i} />)
            : filtered.map((fund) => <FundCard key={fund.fund_id} fund={fund} />)
          }
        </div>
      )}
    </div>
  )
}

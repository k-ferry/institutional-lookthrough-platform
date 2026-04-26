import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import { Globe, ChevronUp, ChevronDown, ChevronsUpDown, AlertCircle } from 'lucide-react'

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

const COUNTRY_CODES = {
  'United States': 'US',
  'United Kingdom': 'GB',
  'Germany': 'DE',
  'Japan': 'JP',
  'Canada': 'CA',
  'France': 'FR',
  'Australia': 'AU',
  'Sweden': 'SE',
  'Switzerland': 'CH',
  'Netherlands': 'NL',
}

const COUNTRY_COLORS = {
  'United States': '#1A4B9B',
  'United Kingdom': '#1e40af',
  'Germany': '#0f766e',
  'Japan': '#0369a1',
  'Canada': '#7c3aed',
  'France': '#b45309',
  'Australia': '#047857',
  'Sweden': '#9333ea',
  'Switzerland': '#c026d3',
  'Netherlands': '#0891b2',
  'Unknown': '#94a3b8',
}

const TEAL_FALLBACK = '#0d9488'

function getColor(country) {
  return COUNTRY_COLORS[country] ?? TEAL_FALLBACK
}

function flagLabel(country) {
  return country
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

function StatCard({ title, value, sub }) {
  return (
    <Card>
      <CardContent className="p-6">
        <p className="text-sm font-medium text-secondary-500">{title}</p>
        <p className="text-2xl font-bold text-secondary-900 mt-1">{value ?? '—'}</p>
        {sub && <p className="text-xs text-secondary-400 mt-1">{sub}</p>}
      </CardContent>
    </Card>
  )
}

function SortIcon({ col, sortKey, sortDir }) {
  if (sortKey !== col) return <ChevronsUpDown className="h-3 w-3 text-secondary-300 ml-1 inline" />
  return sortDir === 'desc'
    ? <ChevronDown className="h-3 w-3 text-primary-600 ml-1 inline" />
    : <ChevronUp className="h-3 w-3 text-primary-600 ml-1 inline" />
}

function GeographyChart({ data }) {
  const chartHeight = Math.max(300, data.length * 38)
  return (
    <ResponsiveContainer width="100%" height={chartHeight}>
      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 48, top: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
        <XAxis
          type="number"
          dataKey="value"
          tick={{ fontSize: 11, fill: '#64748b' }}
          axisLine={false}
          tickLine={false}
          tickFormatter={(v) => {
            if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`
            if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`
            if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
            return `$${v}`
          }}
          label={{
            value: 'AUM',
            position: 'insideBottomRight',
            offset: -4,
            fontSize: 10,
            fill: '#94a3b8',
          }}
        />
        <YAxis
          type="category"
          dataKey="label"
          width={145}
          tick={{ fontSize: 11, fill: '#475569' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value, _name, props) => {
            const { holding_count, pct, fullName } = props.payload
            const countStr = holding_count != null ? ` · ${holding_count.toLocaleString()} holdings` : ''
            return [`${formatAUM(value)}${countStr} (${pct}%)`, fullName]
          }}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} maxBarSize={26}>
          {data.map((entry, i) => (
            <Cell key={i} fill={entry.color} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function GeographyPage() {
  const [sortKey, setSortKey] = useState('holding_count')
  const [sortDir, setSortDir] = useState('desc')

  const { data, isLoading, error } = useQuery({
    queryKey: ['geography-breakdown'],
    queryFn: () => fetchJSON('/api/dashboard/geography-breakdown'),
    staleTime: 5 * 60 * 1000,
  })

  const totalHoldings = data?.total_holdings ?? 0

  // Recalculate percentage client-side from total_value so it reflects value share,
  // not holding count share (the API percentage is count-based).
  const _rawGeos = (data?.geographies ?? []).filter(g => {
    const c = String(g.geography ?? '').trim()
    return c && c !== 'NaN' && c !== 'null' && c !== 'undefined'
  })
  const _valueTotal = _rawGeos.reduce((sum, g) => sum + (Number(g.total_value) || 0), 0)
  const geographies = _rawGeos.map((g) => ({
    ...g,
    percentage: _valueTotal > 0
      ? parseFloat(((Number(g.total_value) || 0) / _valueTotal * 100).toFixed(1))
      : 0,
  }))

  const unknownGeo = geographies.find((g) => g.geography === 'Unknown')
  const knownGeos = geographies.filter((g) => g.geography !== 'Unknown')

  const holdingsWithData = totalHoldings - (unknownGeo?.holding_count ?? 0)
  const pctWithData = totalHoldings > 0
    ? ((holdingsWithData / totalHoldings) * 100).toFixed(1)
    : '0'

  // Largest country by total_value (matches value-based percentage)
  const largestKnown = knownGeos.length > 0
    ? [...knownGeos].sort((a, b) => (Number(b.total_value) || 0) - (Number(a.total_value) || 0))[0]
    : null

  // Sort table rows; Unknown is always pinned to the bottom
  function toggleSort(key) {
    if (sortKey === key) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const sortedKnown = [...knownGeos].sort((a, b) => {
    const va = isNaN(Number(a[sortKey])) ? -1 : Number(a[sortKey])
    const vb = isNaN(Number(b[sortKey])) ? -1 : Number(b[sortKey])
    return sortDir === 'desc' ? vb - va : va - vb
  })
  const tableRows = unknownGeo ? [...sortedKnown, unknownGeo] : sortedKnown

  // Chart data: all countries sorted by value DESC (Unknown included inline, not pinned).
  const chartData = [...geographies]
    .filter((g) => g.holding_count > 0)
    .sort((a, b) => (Number(b.total_value) || 0) - (Number(a.total_value) || 0))
    .map((g) => ({
      label: flagLabel(g.geography).length > 22
        ? flagLabel(g.geography).slice(0, 20) + '…'
        : flagLabel(g.geography),
      fullName: g.geography,
      value: Number(g.total_value) || 0,
      holding_count: Number(g.holding_count) || 0,
      pct: Number(g.percentage) || 0,
      color: getColor(g.geography),
    }))

  const thClass = (col) =>
    `text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide cursor-pointer select-none hover:text-secondary-800 whitespace-nowrap`

  // ---- Loading skeleton ----
  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-8 w-64 bg-secondary-200 rounded animate-pulse" />
        <div className="grid grid-cols-3 gap-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="h-24 bg-secondary-100 rounded-lg animate-pulse" />
          ))}
        </div>
        <div className="h-64 bg-secondary-100 rounded-lg animate-pulse" />
      </div>
    )
  }

  if (error) {
    return <ErrorBanner message={`Failed to load geography data: ${error.message}`} />
  }

  return (
    <div className="space-y-6">

      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-secondary-900">Geographic Exposure</h1>
          <p className="text-secondary-500 mt-1">
            {knownGeos.length} {knownGeos.length === 1 ? 'country' : 'countries'} identified
            across {totalHoldings.toLocaleString()} holdings
          </p>
        </div>
        <div className="h-10 w-10 rounded-full bg-primary-50 flex items-center justify-center">
          <Globe className="h-5 w-5 text-primary-600" />
        </div>
      </div>

      {/* Summary stat cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatCard
          title="Total Countries"
          value={knownGeos.length.toLocaleString()}
          sub="Distinct geographies identified"
        />
        <StatCard
          title="Holdings with Country Data"
          value={holdingsWithData.toLocaleString()}
          sub={`${pctWithData}% of all holdings`}
        />
        <StatCard
          title="Largest Country Exposure"
          value={largestKnown ? flagLabel(largestKnown.geography) : '—'}
          sub={largestKnown ? `${largestKnown.percentage}% of portfolio value` : undefined}
        />
      </div>

      {/* Bar chart */}
      <Card>
        <CardHeader>
          <CardTitle>AUM by Country</CardTitle>
        </CardHeader>
        <CardContent>
          {chartData.length ? (
            <GeographyChart data={chartData} />
          ) : (
            <div className="h-48 flex items-center justify-center text-secondary-400 text-sm">
              No geography data available
            </div>
          )}
        </CardContent>
      </Card>

      {/* Country breakdown table */}
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle>Country Breakdown</CardTitle>
        </CardHeader>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide w-8">
                  {/* color swatch */}
                </th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Country
                </th>
                <th
                  className={thClass('holding_count')}
                  onClick={() => toggleSort('holding_count')}
                >
                  Holdings
                  <SortIcon col="holding_count" sortKey={sortKey} sortDir={sortDir} />
                </th>
                <th
                  className={thClass('company_count')}
                  onClick={() => toggleSort('company_count')}
                >
                  Companies
                  <SortIcon col="company_count" sortKey={sortKey} sortDir={sortDir} />
                </th>
                <th
                  className={thClass('total_value')}
                  onClick={() => toggleSort('total_value')}
                >
                  AUM
                  <SortIcon col="total_value" sortKey={sortKey} sortDir={sortDir} />
                </th>
                <th
                  className={thClass('percentage')}
                  onClick={() => toggleSort('percentage')}
                >
                  % Portfolio
                  <SortIcon col="percentage" sortKey={sortKey} sortDir={sortDir} />
                </th>
              </tr>
            </thead>
            <tbody>
              {tableRows.map((row) => {
                const isUnknown = row.geography === 'Unknown'
                return (
                  <tr
                    key={row.geography}
                    className={`border-b border-secondary-100 hover:bg-secondary-50 transition-colors ${isUnknown ? 'bg-secondary-50/60' : ''}`}
                  >
                    <td className="py-3 px-4">
                      <span
                        className="inline-block w-3 h-3 rounded-sm"
                        style={{ backgroundColor: getColor(row.geography) }}
                      />
                    </td>
                    <td className="py-3 px-4 font-medium text-secondary-800">
                      {isUnknown ? (
                        <span className="text-secondary-400 italic">{flagLabel(row.geography)}</span>
                      ) : (
                        flagLabel(row.geography)
                      )}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-secondary-700">
                      {row.holding_count.toLocaleString()}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-secondary-600">
                      {row.company_count.toLocaleString()}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                      {row.total_value != null && !isNaN(row.total_value) && row.total_value > 0 ? formatAUM(row.total_value) : '—'}
                    </td>
                    <td className="py-3 px-4 text-right">
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                        isUnknown
                          ? 'bg-secondary-100 text-secondary-500'
                          : 'bg-primary-50 text-primary-700'
                      }`}>
                        {row.percentage}%
                      </span>
                    </td>
                  </tr>
                )
              })}
              {!tableRows.length && (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-secondary-400 text-sm">
                    No geography data available.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Data quality note */}
      {totalHoldings > 0 && (
        <div className="flex items-start gap-2 rounded-lg bg-secondary-50 border border-secondary-200 px-4 py-3 text-xs text-secondary-500">
          <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5 text-secondary-400" />
          <span>
            Geography data available for{' '}
            <span className="font-medium text-secondary-700">{holdingsWithData.toLocaleString()}</span>
            {' '}of{' '}
            <span className="font-medium text-secondary-700">{totalHoldings.toLocaleString()}</span>
            {' '}holdings ({pctWithData}%).
            BDC holdings use reported country from SEC filings.
            Remaining {unknownGeo ? unknownGeo.holding_count.toLocaleString() : '0'} holdings classified as Unknown.
          </span>
        </div>
      )}
    </div>
  )
}

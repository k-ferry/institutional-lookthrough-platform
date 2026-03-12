import { useEffect, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  BarChart2,
  ChevronRight,
  AlertCircle,
  ChevronLeft,
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
} from 'lucide-react'

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

// Map sector names to border accent colors
const SECTOR_COLORS = {
  'Technology': '#1A4B9B',
  'Healthcare': '#0891b2',
  'Financial Services': '#0f766e',
  'Industrials': '#7c3aed',
  'Consumer Discretionary': '#b45309',
  'Communication Services': '#0369a1',
  'Energy': '#c026d3',
  'Materials': '#047857',
  'Consumer Staples': '#9333ea',
  'Real Estate': '#dc2626',
  'Utilities': '#d97706',
  'Unclassified': '#94a3b8',
}
const DEFAULT_SECTOR_COLOR = '#64748b'

function sectorColor(name) {
  return SECTOR_COLORS[name] ?? DEFAULT_SECTOR_COLOR
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

function LoadingRows({ count = 5 }) {
  return (
    <>
      {[...Array(count)].map((_, i) => (
        <div key={i} className="flex items-center gap-3 py-3 animate-pulse">
          <div className="h-3 bg-secondary-200 rounded flex-1" style={{ width: `${60 - i * 8}%` }} />
          <div className="h-3 w-16 bg-secondary-100 rounded" />
        </div>
      ))}
    </>
  )
}

// Breadcrumb nav
function Breadcrumb({ sector, industry, sub, onSector, onIndustry, onRoot }) {
  return (
    <nav className="flex items-center gap-1.5 text-sm text-secondary-500 flex-wrap">
      <button
        onClick={onRoot}
        className="hover:text-primary-700 font-medium transition-colors"
      >
        All Sectors
      </button>
      {sector && (
        <>
          <ChevronRight className="h-3.5 w-3.5 text-secondary-300 shrink-0" />
          <button
            onClick={onSector}
            className={`font-medium transition-colors ${
              !industry ? 'text-secondary-900 cursor-default' : 'hover:text-primary-700'
            }`}
            disabled={!industry}
          >
            {sector}
          </button>
        </>
      )}
      {industry && (
        <>
          <ChevronRight className="h-3.5 w-3.5 text-secondary-300 shrink-0" />
          <button
            onClick={onIndustry}
            className={`font-medium transition-colors ${
              !sub ? 'text-secondary-900 cursor-default' : 'hover:text-primary-700'
            }`}
            disabled={!sub}
          >
            {industry}
          </button>
        </>
      )}
      {sub && (
        <>
          <ChevronRight className="h-3.5 w-3.5 text-secondary-300 shrink-0" />
          <span className="font-medium text-secondary-900">{sub}</span>
        </>
      )}
    </nav>
  )
}

// Progress bar row for industry groups / sub-industries
function TaxonomyRow({ label, holdingCount, totalCount, value, onClick }) {
  const pct = totalCount > 0 ? (holdingCount / totalCount) * 100 : 0
  return (
    <button
      className="w-full text-left group"
      onClick={onClick}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="text-sm font-medium text-secondary-800 group-hover:text-primary-700 transition-colors">
          {label}
        </span>
        <span className="text-xs text-secondary-500 ml-2 shrink-0">
          {holdingCount.toLocaleString()} holdings
          {value != null ? ` · ${formatAUM(value)}` : ''}
        </span>
      </div>
      <div className="h-1.5 bg-secondary-100 rounded-full overflow-hidden">
        <div
          className="h-full bg-primary-500 rounded-full group-hover:bg-primary-600 transition-colors"
          style={{ width: `${Math.max(pct, 1)}%` }}
        />
      </div>
    </button>
  )
}

// Top company table
function TopCompaniesTable({ companies }) {
  if (!companies?.length) return null
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-secondary-200">
            <th className="text-left py-2 px-3 font-semibold text-secondary-500 text-xs uppercase tracking-wide">Company</th>
            <th className="text-right py-2 px-3 font-semibold text-secondary-500 text-xs uppercase tracking-wide">Holdings</th>
            <th className="text-right py-2 px-3 font-semibold text-secondary-500 text-xs uppercase tracking-wide">AUM</th>
          </tr>
        </thead>
        <tbody>
          {companies.map((c) => (
            <tr key={c.company_id ?? c.company_name} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
              <td className="py-2 px-3 font-medium text-secondary-800">
                {c.company_id ? (
                  <Link
                    to={`/companies/${c.company_id}`}
                    className="text-primary-700 hover:text-primary-900 hover:underline"
                  >
                    {c.company_name ?? '—'}
                  </Link>
                ) : (
                  c.company_name ?? '—'
                )}
              </td>
              <td className="py-2 px-3 text-right tabular-nums text-secondary-600">
                {c.holding_count?.toLocaleString() ?? '—'}
              </td>
              <td className="py-2 px-3 text-right tabular-nums font-medium text-secondary-800">
                {formatAUM(c.total_value)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Sort icon
function SortIcon({ col, sortKey, sortDir }) {
  if (sortKey !== col) return <ChevronsUpDown className="h-3 w-3 text-secondary-300 ml-1 inline" />
  return sortDir === 'desc'
    ? <ChevronDown className="h-3 w-3 text-primary-600 ml-1 inline" />
    : <ChevronUp className="h-3 w-3 text-primary-600 ml-1 inline" />
}

// ---------------------------------------------------------------------------
// Level 0 — All Sectors
// ---------------------------------------------------------------------------

function SectorsLevel({ onSelect }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['gics-sectors'],
    queryFn: () => fetchJSON('/api/gics/sectors'),
    staleTime: 5 * 60 * 1000,
  })

  if (error) return <ErrorBanner message={`Failed to load sectors: ${error.message}`} />

  const sectors = data?.sectors ?? []
  const total = data?.total_holdings ?? 1

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Sector Explorer</h1>
        <p className="text-secondary-500 mt-1">
          {isLoading ? 'Loading…' : `${sectors.length} sectors across ${(total).toLocaleString()} holdings`}
        </p>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="h-32 bg-secondary-100 rounded-lg animate-pulse" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {sectors.map((s) => {
            const color = sectorColor(s.sector)
            const pct = total > 0 ? ((s.holding_count / total) * 100).toFixed(1) : '0'
            return (
              <button
                key={s.sector}
                onClick={() => onSelect(s.sector)}
                className="text-left rounded-lg border border-secondary-200 bg-white p-5 hover:border-primary-300 hover:shadow-sm transition-all group"
                style={{ borderLeftWidth: 4, borderLeftColor: color }}
              >
                <div className="flex items-start justify-between gap-2">
                  <p className="font-semibold text-secondary-900 group-hover:text-primary-700 transition-colors leading-tight">
                    {s.sector}
                  </p>
                  <ChevronRight className="h-4 w-4 text-secondary-300 group-hover:text-primary-500 shrink-0 mt-0.5 transition-colors" />
                </div>
                <div className="mt-3 flex items-end justify-between">
                  <div>
                    <p className="text-2xl font-bold text-secondary-800 tabular-nums">
                      {s.holding_count.toLocaleString()}
                    </p>
                    <p className="text-xs text-secondary-400 mt-0.5">holdings</p>
                  </div>
                  <div className="text-right">
                    <span className="inline-flex items-center rounded-full bg-primary-50 px-2 py-0.5 text-xs font-medium text-primary-700">
                      {pct}%
                    </span>
                    {s.total_value != null && (
                      <p className="text-xs text-secondary-400 mt-1">{formatAUM(s.total_value)}</p>
                    )}
                  </div>
                </div>
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Level 1 — Sector Detail (Industry Groups)
// ---------------------------------------------------------------------------

function SectorLevel({ sector, onSelectIndustry }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['gics-sector', sector],
    queryFn: () => fetchJSON(`/api/gics/sector/${encodeURIComponent(sector)}`),
    staleTime: 5 * 60 * 1000,
  })

  if (error) return <ErrorBanner message={`Failed to load sector: ${error.message}`} />

  const groups = data?.industry_groups ?? []
  const topCompanies = data?.top_companies ?? []
  const totalHoldings = data?.holding_count ?? 1

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { label: 'Holdings', value: isLoading ? '…' : (data?.holding_count ?? 0).toLocaleString() },
          { label: 'Companies', value: isLoading ? '…' : (data?.company_count ?? 0).toLocaleString() },
          { label: 'AUM', value: isLoading ? '…' : formatAUM(data?.total_value) },
        ].map(({ label, value }) => (
          <Card key={label}>
            <CardContent className="p-5">
              <p className="text-xs font-medium uppercase tracking-wide text-secondary-400">{label}</p>
              <p className="text-2xl font-bold text-secondary-900 mt-1">{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Industry Groups */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Industry Groups</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {isLoading ? (
              <LoadingRows />
            ) : groups.length ? (
              groups.map((g) => (
                <TaxonomyRow
                  key={g.industry_group}
                  label={g.industry_group}
                  holdingCount={g.holding_count}
                  totalCount={totalHoldings}
                  value={g.total_value}
                  onClick={() => onSelectIndustry(g.industry_group)}
                />
              ))
            ) : (
              <p className="text-sm text-secondary-400">No industry group data available.</p>
            )}
          </CardContent>
        </Card>

        {/* Top Companies */}
        <Card>
          <CardHeader>
            <CardTitle>Top Companies</CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <LoadingRows />
            ) : (
              <TopCompaniesTable companies={topCompanies} />
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Level 2 — Industry Group Detail (Sub-industries)
// ---------------------------------------------------------------------------

function IndustryLevel({ industry, onSelectSub }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['gics-industry', industry],
    queryFn: () => fetchJSON(`/api/gics/industry/${encodeURIComponent(industry)}`),
    staleTime: 5 * 60 * 1000,
  })

  if (error) return <ErrorBanner message={`Failed to load industry group: ${error.message}`} />

  const subs = data?.sub_industries ?? []
  const topCompanies = data?.top_companies ?? []
  const totalHoldings = data?.holding_count ?? 1

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { label: 'Holdings', value: isLoading ? '…' : (data?.holding_count ?? 0).toLocaleString() },
          { label: 'Companies', value: isLoading ? '…' : (data?.company_count ?? 0).toLocaleString() },
          { label: 'AUM', value: isLoading ? '…' : formatAUM(data?.total_value) },
        ].map(({ label, value }) => (
          <Card key={label}>
            <CardContent className="p-5">
              <p className="text-xs font-medium uppercase tracking-wide text-secondary-400">{label}</p>
              <p className="text-2xl font-bold text-secondary-900 mt-1">{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Sub-industries + Top Companies */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Industries</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {isLoading ? (
              <LoadingRows />
            ) : subs.length ? (
              subs.map((s) => (
                <TaxonomyRow
                  key={s.industry}
                  label={s.industry}
                  holdingCount={s.holding_count}
                  totalCount={totalHoldings}
                  value={s.total_value}
                  onClick={() => onSelectSub(s.industry)}
                />
              ))
            ) : (
              <p className="text-sm text-secondary-400">No industry data available.</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top Companies</CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? <LoadingRows /> : <TopCompaniesTable companies={topCompanies} />}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Level 3 — Holdings Table
// ---------------------------------------------------------------------------

function HoldingsLevel({ sector, industry, sub }) {
  const [page, setPage] = useState(1)
  const [sortKey, setSortKey] = useState('reported_value_usd')
  const [sortDir] = useState('desc')
  const PAGE_SIZE = 50

  // Reset page when filters change
  useEffect(() => { setPage(1) }, [sector, industry, sub])

  const params = new URLSearchParams({ page, page_size: PAGE_SIZE })
  if (sector) params.set('sector', sector)
  if (industry) params.set('industry', industry)
  if (sub) params.set('sub_industry', sub)

  const { data, isLoading, error, placeholderData } = useQuery({
    queryKey: ['gics-holdings', sector, industry, sub, page],
    queryFn: () => fetchJSON(`/api/gics/holdings?${params}`),
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  })

  const items = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = data?.total_pages ?? 1
  const isStale = placeholderData && isLoading

  const thClass = 'text-left py-3 px-3 font-semibold text-secondary-500 text-xs uppercase tracking-wide cursor-pointer select-none hover:text-secondary-800 whitespace-nowrap'

  if (error) return <ErrorBanner message={`Failed to load holdings: ${error.message}`} />

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-secondary-600">
          {isLoading && !placeholderData ? 'Loading…' : `${total.toLocaleString()} holdings`}
          {sub ? ` in ${sub}` : industry ? ` in ${industry}` : sector ? ` in ${sector}` : ''}
        </p>
      </div>

      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className={`w-full text-sm transition-opacity ${isStale ? 'opacity-60' : ''}`}>
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <th className={thClass}>Company</th>
                <th className={thClass}>Fund</th>
                <th className={`${thClass} text-right`}>Value</th>
                <th className={thClass}>Sector</th>
                <th className={thClass}>Date</th>
                <th className={thClass}>Source</th>
              </tr>
            </thead>
            <tbody>
              {isLoading && !placeholderData ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-secondary-400 text-sm">
                    Loading holdings…
                  </td>
                </tr>
              ) : items.length ? (
                items.map((r) => (
                  <tr key={r.holding_id} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
                    <td className="py-2.5 px-3 max-w-[200px]">
                      {r.company_id ? (
                        <Link
                          to={`/companies/${r.company_id}`}
                          className="font-medium text-primary-700 hover:text-primary-900 hover:underline truncate block"
                        >
                          {r.company_name ?? '—'}
                        </Link>
                      ) : (
                        <span className="font-medium text-secondary-700 truncate block">{r.company_name ?? '—'}</span>
                      )}
                    </td>
                    <td className="py-2.5 px-3 max-w-[160px]">
                      {r.fund_id ? (
                        <Link
                          to={`/funds/${r.fund_id}`}
                          className="text-secondary-600 hover:text-primary-700 hover:underline truncate block text-xs"
                        >
                          {r.fund_name ?? '—'}
                        </Link>
                      ) : (
                        <span className="text-secondary-500 text-xs truncate block">{r.fund_name ?? '—'}</span>
                      )}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums font-medium text-secondary-800 whitespace-nowrap">
                      {r.reported_value_usd != null ? formatAUM(r.reported_value_usd) : '—'}
                    </td>
                    <td className="py-2.5 px-3">
                      <span className="inline-flex items-center rounded-full bg-secondary-100 px-2 py-0.5 text-xs text-secondary-600">
                        {r.reported_sector ?? '—'}
                      </span>
                    </td>
                    <td className="py-2.5 px-3 text-secondary-500 text-xs whitespace-nowrap">
                      {r.as_of_date ?? '—'}
                    </td>
                    <td className="py-2.5 px-3">
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                        r.source === 'synthetic'
                          ? 'bg-violet-50 text-violet-700'
                          : 'bg-amber-50 text-amber-700'
                      }`}>
                        {r.source === 'synthetic' ? 'Synthetic' : 'BDC'}
                      </span>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-secondary-400 text-sm">
                    No holdings found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-secondary-200 bg-secondary-50">
            <p className="text-xs text-secondary-500">
              Page {page} of {totalPages} · {total.toLocaleString()} total
            </p>
            <div className="flex gap-2">
              <button
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded border border-secondary-300 text-secondary-600 hover:bg-secondary-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronLeft className="h-3 w-3" /> Prev
              </button>
              <button
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded border border-secondary-300 text-secondary-600 hover:bg-secondary-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Next <ChevronRight className="h-3 w-3" />
              </button>
            </div>
          </div>
        )}
      </Card>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page root — URL-driven drill state
// ---------------------------------------------------------------------------

export default function GICSPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()

  const sector = searchParams.get('sector') || ''
  const industry = searchParams.get('industry') || ''
  const sub = searchParams.get('sub') || ''

  // Derive current drill level
  const level = sub ? 3 : industry ? 2 : sector ? 1 : 0

  function goRoot() { setSearchParams({}) }
  function goSector() { setSearchParams({ sector }) }
  function goIndustry() { setSearchParams({ sector, industry }) }
  function selectSector(name) { setSearchParams({ sector: name }) }
  function selectIndustry(name) { setSearchParams({ sector, industry: name }) }
  function selectSub(name) { setSearchParams({ sector, industry, sub: name }) }

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-start justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <BarChart2 className="h-5 w-5 text-primary-600 shrink-0" />
            <h1 className="text-2xl font-bold text-secondary-900">
              {sub || industry || sector || 'Sector Explorer'}
            </h1>
          </div>
          <Breadcrumb
            sector={sector}
            industry={industry}
            sub={sub}
            onRoot={goRoot}
            onSector={goSector}
            onIndustry={goIndustry}
          />
        </div>
        {level > 0 && (
          <button
            onClick={() => {
              if (level === 3) goIndustry()
              else if (level === 2) goSector()
              else goRoot()
            }}
            className="flex items-center gap-1.5 text-sm text-secondary-500 hover:text-secondary-800 transition-colors shrink-0 mt-1"
          >
            <ChevronLeft className="h-4 w-4" />
            Back
          </button>
        )}
      </div>

      {/* Drill levels */}
      {level === 0 && <SectorsLevel onSelect={selectSector} />}
      {level === 1 && (
        <>
          <SectorLevel sector={sector} onSelectIndustry={selectIndustry} />
          <div className="border-t border-secondary-200 pt-6">
            <h2 className="text-base font-semibold text-secondary-700 mb-4">All Holdings in {sector}</h2>
            <HoldingsLevel sector={sector} />
          </div>
        </>
      )}
      {level === 2 && (
        <>
          <IndustryLevel industry={industry} onSelectSub={selectSub} />
          <div className="border-t border-secondary-200 pt-6">
            <h2 className="text-base font-semibold text-secondary-700 mb-4">All Holdings in {industry}</h2>
            <HoldingsLevel sector={sector} industry={industry} />
          </div>
        </>
      )}
      {level === 3 && (
        <div>
          <h2 className="text-base font-semibold text-secondary-700 mb-4">Holdings in {sub}</h2>
          <HoldingsLevel sector={sector} industry={industry} sub={sub} />
        </div>
      )}
    </div>
  )
}

import { useState, useEffect, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  AlertCircle,
} from 'lucide-react'
import { Card, CardContent } from '../components/ui/card'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function buildHoldingsUrl(filters) {
  const params = new URLSearchParams()
  params.set('page', String(filters.page))
  params.set('page_size', String(filters.pageSize))
  if (filters.search) params.set('search', filters.search)
  if (filters.fundId) params.set('fund_id', filters.fundId)
  if (filters.sector) params.set('sector', filters.sector)
  if (filters.hasValue) params.set('has_value', 'true')
  return `/api/holdings?${params.toString()}`
}

function formatCurrency(value) {
  if (value === null || value === undefined) return '—'
  if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`
  return `$${value.toLocaleString()}`
}

// ---------------------------------------------------------------------------
// Sector pill colour map
// ---------------------------------------------------------------------------

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

function SkeletonRows({ count = 10 }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <tr key={i} className="border-b border-secondary-100">
          <td className="py-3 px-4">
            <div className="h-3.5 w-40 bg-secondary-200 rounded animate-pulse" />
          </td>
          <td className="py-3 px-4">
            <div className="h-3 w-32 bg-secondary-100 rounded animate-pulse" />
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
      ))}
    </>
  )
}

function EmptyState() {
  return (
    <tr>
      <td colSpan={7} className="py-16 text-center">
        <p className="text-secondary-400 text-sm">No holdings match your filters.</p>
        <p className="text-secondary-300 text-xs mt-1">Try clearing some filters to see more results.</p>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

function Pagination({ page, totalPages, onPageChange }) {
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
          title="First page"
        >
          <ChevronsLeft className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(page - 1)}
          disabled={page === 1}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
          title="Previous page"
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(page + 1)}
          disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
          title="Next page"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
        <button
          onClick={() => onPageChange(totalPages)}
          disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600"
          title="Last page"
        >
          <ChevronsRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

const DEFAULT_FILTERS = {
  search: '',
  fundId: '',
  sector: '',
  hasValue: false,
  page: 1,
  pageSize: 50,
}

export default function HoldingsPage() {
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [searchInput, setSearchInput] = useState('')

  // Debounce the search field — 300ms
  useEffect(() => {
    const timer = setTimeout(() => {
      setFilters((prev) => ({ ...prev, search: searchInput, page: 1 }))
    }, 300)
    return () => clearTimeout(timer)
  }, [searchInput])

  const setFilter = useCallback((key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }))
  }, [])

  const clearFilters = useCallback(() => {
    setSearchInput('')
    setFilters(DEFAULT_FILTERS)
  }, [])

  const hasActiveFilters =
    filters.search || filters.fundId || filters.sector || filters.hasValue

  // Filter options query
  const { data: filterOpts } = useQuery({
    queryKey: ['holdings-filters'],
    queryFn: () => fetchJSON('/api/holdings/filters'),
    staleTime: 10 * 60 * 1000,
  })

  // Holdings data query — key includes all filter params so each combination is cached
  const {
    data,
    isLoading,
    isFetching,
    error,
  } = useQuery({
    queryKey: [
      'holdings',
      filters.page,
      filters.pageSize,
      filters.search,
      filters.fundId,
      filters.sector,
      filters.hasValue,
    ],
    queryFn: () => fetchJSON(buildHoldingsUrl(filters)),
    placeholderData: (prev) => prev,
    staleTime: 2 * 60 * 1000,
  })

  const total = data?.total ?? 0
  const totalPages = data?.total_pages ?? 1
  const funds = filterOpts?.funds ?? []
  const sectors = filterOpts?.sectors ?? []

  // Subtitle — "6,054 holdings across 11 funds"
  const subtitle =
    filterOpts
      ? `${total.toLocaleString()} holdings across ${funds.length} funds`
      : 'Loading…'

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Holdings Explorer</h1>
        <p className="text-secondary-500 mt-1">{subtitle}</p>
      </div>

      {error && <ErrorBanner message={`Failed to load holdings: ${error.message}`} />}

      {/* Filter bar */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Search */}
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

            {/* Fund dropdown */}
            <select
              value={filters.fundId}
              onChange={(e) => setFilter('fundId', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All Funds</option>
              {funds.map((f) => (
                <option key={f.id} value={f.id}>
                  {f.name}
                </option>
              ))}
            </select>

            {/* Sector dropdown */}
            <select
              value={filters.sector}
              onChange={(e) => setFilter('sector', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All Sectors</option>
              {sectors.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>

            {/* With Value Only */}
            <label className="flex items-center gap-2 text-sm text-secondary-700 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={filters.hasValue}
                onChange={(e) => setFilter('hasValue', e.target.checked)}
                className="h-4 w-4 rounded border-secondary-300 text-primary-600 focus:ring-primary-500"
              />
              With Value Only
            </label>

            {/* Clear filters */}
            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1.5 px-3 py-2 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
              >
                <X className="h-3.5 w-3.5" />
                Clear
              </button>
            )}

            {/* Results count */}
            <span className="ml-auto text-sm text-secondary-500 whitespace-nowrap">
              {isFetching && !isLoading ? (
                <span className="text-secondary-400">Updating…</span>
              ) : (
                <>
                  Showing{' '}
                  <span className="font-medium text-secondary-700">
                    {Math.min(filters.pageSize, total).toLocaleString()}
                  </span>{' '}
                  of{' '}
                  <span className="font-medium text-secondary-700">
                    {total.toLocaleString()}
                  </span>
                </>
              )}
            </span>
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Company
                </th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Fund
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
              {isLoading ? (
                <SkeletonRows count={filters.pageSize > 20 ? 15 : filters.pageSize} />
              ) : !data?.items?.length ? (
                <EmptyState />
              ) : (
                data.items.map((row) => (
                  <tr
                    key={row.holding_id}
                    className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors"
                  >
                    {/* Company */}
                    <td className="py-3 px-4 max-w-[200px]">
                      <span
                        className="font-semibold text-primary-700 truncate block"
                        title={row.company_name ?? '—'}
                      >
                        {row.company_name ?? '—'}
                      </span>
                    </td>

                    {/* Fund */}
                    <td className="py-3 px-4 text-secondary-600 max-w-[160px]">
                      <span className="truncate block" title={row.fund_name ?? '—'}>
                        {row.fund_name ?? '—'}
                      </span>
                    </td>

                    {/* Sector pill */}
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

                    {/* Country */}
                    <td className="py-3 px-4 text-secondary-600">
                      {row.country ?? <span className="text-secondary-300">—</span>}
                    </td>

                    {/* Reported value */}
                    <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                      {row.reported_value != null ? (
                        formatCurrency(row.reported_value)
                      ) : (
                        <span className="text-secondary-300">—</span>
                      )}
                    </td>

                    {/* Date */}
                    <td className="py-3 px-4 text-secondary-500 text-xs">
                      {row.date_reported ?? '—'}
                    </td>

                    {/* Source */}
                    <td className="py-3 px-4">
                      <span className="text-xs text-secondary-400">{row.source ?? '—'}</span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {!isLoading && data && (
          <Pagination
            page={filters.page}
            totalPages={totalPages}
            onPageChange={(p) => setFilters((prev) => ({ ...prev, page: p }))}
          />
        )}
      </Card>
    </div>
  )
}

import { useState, useEffect, useCallback, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  ChevronDown,
  ChevronUp,
  AlertCircle,
  Download,
  Loader2,
  DollarSign,
  Briefcase,
  Globe,
  BarChart2,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
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

// Sector chip color map
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
  Unclassified: 'bg-gray-100 text-gray-500',
}

function sectorChipClasses(sector) {
  if (!sector || sector === 'Unclassified') return 'bg-gray-100 text-gray-500'
  for (const [key, cls] of Object.entries(SECTOR_COLORS)) {
    if (sector.toLowerCase().includes(key.toLowerCase())) return cls
  }
  return 'bg-gray-100 text-gray-500'
}

// ---------------------------------------------------------------------------
// URL builder
// ---------------------------------------------------------------------------

function buildHoldingsUrl(filters) {
  const p = new URLSearchParams()
  p.set('page', String(filters.page))
  p.set('page_size', String(filters.pageSize))
  p.set('sort_by', filters.sortBy)
  p.set('sort_dir', filters.sortDir)
  if (filters.search)    p.set('search', filters.search)
  if (filters.fundId)    p.set('fund_id', filters.fundId)
  if (filters.sector)    p.set('sector', filters.sector)
  if (filters.industry)  p.set('industry', filters.industry)
  if (filters.country)   p.set('country', filters.country)
  if (filters.asOfDate)  p.set('as_of_date', filters.asOfDate)
  return `/api/holdings?${p.toString()}`
}

function buildExportUrl(filters, suffix) {
  const p = new URLSearchParams()
  if (filters.search)   p.set('search', filters.search)
  if (filters.fundId)   p.set('fund_id', filters.fundId)
  if (filters.sector)   p.set('sector', filters.sector)
  if (filters.industry) p.set('industry', filters.industry)
  if (filters.country)  p.set('country', filters.country)
  if (filters.asOfDate) p.set('as_of_date', filters.asOfDate)
  return `/api/holdings${suffix}?${p.toString()}`
}

// ---------------------------------------------------------------------------
// Default filter state
// ---------------------------------------------------------------------------

const DEFAULT_FILTERS = {
  search: '',
  fundId: '',
  sector: '',
  industry: '',
  country: '',
  asOfDate: '',
  page: 1,
  pageSize: 50,
  sortBy: 'reported_value_usd',
  sortDir: 'desc',
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

function StatCard({ title, value, icon: Icon, sub }) {
  return (
    <Card>
      <CardContent className="p-5">
        <div className="flex items-center justify-between">
          <div className="min-w-0">
            <p className="text-xs font-medium text-secondary-500 uppercase tracking-wide">{title}</p>
            <p className="text-2xl font-bold text-secondary-900 mt-1 truncate">{value ?? '—'}</p>
            {sub && <p className="text-xs text-secondary-400 mt-0.5">{sub}</p>}
          </div>
          <div className="h-10 w-10 rounded-full bg-primary-50 flex items-center justify-center shrink-0 ml-3">
            <Icon className="h-5 w-5 text-primary-600" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function SkeletonRows({ count = 12, cols = 8 }) {
  return Array.from({ length: count }).map((_, i) => (
    <tr key={i} className="border-b border-secondary-100">
      {Array.from({ length: cols }).map((_, j) => (
        <td key={j} className="py-3 px-4">
          <div
            className="h-3.5 bg-secondary-200 rounded animate-pulse"
            style={{ width: `${50 + ((i * 13 + j * 17) % 40)}%` }}
          />
        </td>
      ))}
    </tr>
  ))
}

function EmptyState({ cols = 8 }) {
  return (
    <tr>
      <td colSpan={cols} className="py-16 text-center">
        <p className="text-secondary-400 text-sm">No holdings match your filters.</p>
        <p className="text-secondary-300 text-xs mt-1">Try clearing some filters to see more results.</p>
      </td>
    </tr>
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
        <button onClick={() => onPageChange(1)} disabled={page === 1}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600">
          <ChevronsLeft className="h-4 w-4" />
        </button>
        <button onClick={() => onPageChange(page - 1)} disabled={page === 1}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600">
          <ChevronLeft className="h-4 w-4" />
        </button>
        <button onClick={() => onPageChange(page + 1)} disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600">
          <ChevronRight className="h-4 w-4" />
        </button>
        <button onClick={() => onPageChange(totalPages)} disabled={page === totalPages}
          className="p-1.5 rounded hover:bg-secondary-100 disabled:opacity-30 disabled:cursor-not-allowed text-secondary-600">
          <ChevronsRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}

function SortableHeader({ label, field, sortBy, sortDir, onSort, align = 'left' }) {
  const active = sortBy === field
  return (
    <th
      onClick={() => onSort(field)}
      className={`py-3 px-4 font-semibold text-xs uppercase tracking-wide cursor-pointer select-none
        transition-colors text-secondary-600 hover:text-secondary-900
        ${align === 'right' ? 'text-right' : 'text-left'}`}
    >
      <span className={`inline-flex items-center gap-1 ${align === 'right' ? 'flex-row-reverse' : ''}`}>
        {label}
        {active ? (
          sortDir === 'desc'
            ? <ChevronDown className="h-3 w-3 text-primary-600" />
            : <ChevronUp className="h-3 w-3 text-primary-600" />
        ) : (
          <ChevronDown className="h-3 w-3 opacity-20" />
        )}
      </span>
    </th>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function HoldingsPage() {
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [searchInput, setSearchInput] = useState('')
  const [exportOpen, setExportOpen] = useState(false)
  const [isExporting, setIsExporting] = useState(false)
  const exportRef = useRef(null)

  // Close export dropdown on outside click
  useEffect(() => {
    function onClickOutside(e) {
      if (exportRef.current && !exportRef.current.contains(e.target)) setExportOpen(false)
    }
    if (exportOpen) document.addEventListener('mousedown', onClickOutside)
    return () => document.removeEventListener('mousedown', onClickOutside)
  }, [exportOpen])

  async function handleExport(format) {
    setExportOpen(false)
    setIsExporting(true)
    try {
      const suffix = format === 'excel' ? '/export/excel' : '/export'
      const ext = format === 'excel' ? 'xlsx' : 'csv'
      const today = new Date().toISOString().split('T')[0]
      await downloadExport(buildExportUrl(filters, suffix), `lookthrough_holdings_${today}.${ext}`)
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

  const setFilter = useCallback((key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }))
  }, [])

  // Changing sector resets industry
  const handleSectorChange = useCallback((value) => {
    setFilters((prev) => ({ ...prev, sector: value, industry: '', page: 1 }))
  }, [])

  const clearFilters = useCallback(() => {
    setSearchInput('')
    setFilters(DEFAULT_FILTERS)
  }, [])

  function handleSort(field) {
    setFilters((prev) => ({
      ...prev,
      sortBy: field,
      sortDir: prev.sortBy === field && prev.sortDir === 'desc' ? 'asc' : 'desc',
      page: 1,
    }))
  }

  // Filter options (global metadata)
  const { data: filterOpts } = useQuery({
    queryKey: ['holdings-filters'],
    queryFn: () => fetchJSON('/api/holdings/filters'),
    staleTime: 10 * 60 * 1000,
  })

  // Holdings page data
  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: [
      'holdings',
      filters.page, filters.pageSize,
      filters.search, filters.fundId, filters.sector, filters.industry,
      filters.country, filters.asOfDate,
      filters.sortBy, filters.sortDir,
    ],
    queryFn: () => fetchJSON(buildHoldingsUrl(filters)),
    placeholderData: (prev) => prev,
    staleTime: 2 * 60 * 1000,
  })

  // Derive values
  const filteredTotal = data?.total ?? 0
  const totalPages = data?.total_pages ?? 1
  const globalTotal = filterOpts?.total_holdings ?? 0
  const globalExposure = filterOpts?.total_exposure ?? 0
  const fundsCount = filterOpts?.funds?.length ?? 0
  const countriesCount = filterOpts?.countries?.length ?? 0
  const allIndustries = filterOpts?.industries ?? []
  const allFunds = filterOpts?.funds ?? []
  const allSectors = filterOpts?.sectors ?? []
  const allCountries = filterOpts?.countries ?? []
  const allDates = filterOpts?.dates ?? []

  // Industry list cascades from selected sector
  const availableIndustries = filters.sector
    ? allIndustries.filter((ind) => ind.sector === filters.sector)
    : allIndustries

  // Active filter count (excluding pagination/sort)
  const activeFilterCount = [
    filters.sector, filters.industry,
    filters.country, filters.fundId, filters.asOfDate, filters.search,
  ].filter(Boolean).length

  // Results summary chips
  const activeFilterLabels = [
    filters.sector && filters.sector,
    filters.industry && filters.industry,
    filters.country && filters.country,
    filters.fundId && (allFunds.find((f) => f.id === filters.fundId)?.name ?? filters.fundId),
    filters.asOfDate && filters.asOfDate,
    filters.search && `"${filters.search}"`,
  ].filter(Boolean)

  return (
    <div className="space-y-5">

      {/* Page title */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Holdings Explorer</h1>
        <p className="text-secondary-500 mt-1 text-sm">
          {globalTotal.toLocaleString()} total holdings across {fundsCount} funds
        </p>
      </div>

      {error && <ErrorBanner message={`Failed to load holdings: ${error.message}`} />}

      {/* Stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Total Holdings"
          value={globalTotal.toLocaleString()}
          icon={Briefcase}
          sub="All sources"
        />
        <StatCard
          title="Total Exposure"
          value={formatAUM(globalExposure)}
          icon={DollarSign}
          sub="Sum of reported values"
        />
        <StatCard
          title="Funds"
          value={fundsCount.toLocaleString()}
          icon={BarChart2}
          sub="In portfolio"
        />
        <StatCard
          title="Countries"
          value={countriesCount.toLocaleString()}
          icon={Globe}
          sub="Geographic exposure"
        />
      </div>

      {/* Filter panel */}
      <Card>
        <CardContent className="p-4 space-y-3">

          {/* Dropdowns + search row */}
          <div className="flex flex-wrap items-center gap-2">
            {/* Search */}
            <div className="relative min-w-[200px] flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-secondary-400" />
              <input
                type="text"
                placeholder="Search company name…"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                className="w-full pl-9 pr-3 py-2 text-sm border border-secondary-200 rounded-md bg-white text-secondary-900 placeholder-secondary-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>

            {/* Sector */}
            <select
              value={filters.sector}
              onChange={(e) => handleSectorChange(e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 max-w-[160px]"
            >
              <option value="">All Sectors</option>
              {allSectors.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>

            {/* Industry (cascades from sector) */}
            <select
              value={filters.industry}
              onChange={(e) => setFilter('industry', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 max-w-[200px]"
            >
              <option value="">All Industries</option>
              {availableIndustries.map((ind) => (
                <option key={ind.name} value={ind.name}>{ind.name}</option>
              ))}
            </select>

            {/* Country */}
            <select
              value={filters.country}
              onChange={(e) => setFilter('country', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 max-w-[140px]"
            >
              <option value="">All Countries</option>
              {allCountries.map((c) => (
                <option key={c} value={c}>{c}</option>
              ))}
            </select>

            {/* Fund */}
            <select
              value={filters.fundId}
              onChange={(e) => setFilter('fundId', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 max-w-[180px]"
            >
              <option value="">All Funds</option>
              {allFunds.map((f) => (
                <option key={f.id} value={f.id}>{f.name}</option>
              ))}
            </select>

            {/* Date */}
            <select
              value={filters.asOfDate}
              onChange={(e) => setFilter('asOfDate', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 max-w-[140px]"
            >
              <option value="">All Dates</option>
              {allDates.map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
            </select>

            {/* Clear + active count */}
            {activeFilterCount > 0 && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1.5 px-3 py-2 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
              >
                <X className="h-3.5 w-3.5" />
                Clear
                <span className="ml-0.5 inline-flex items-center justify-center h-4 w-4 rounded-full bg-primary-600 text-white text-xs font-bold leading-none">
                  {activeFilterCount}
                </span>
              </button>
            )}

            {/* Export */}
            <div className="relative ml-auto" ref={exportRef}>
              <button
                onClick={() => setExportOpen((o) => !o)}
                disabled={isExporting}
                className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-secondary-700 border border-secondary-200 rounded-md bg-white hover:bg-secondary-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {isExporting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Download className="h-3.5 w-3.5" />}
                Export
                <ChevronDown className="h-3 w-3 text-secondary-400" />
              </button>
              {exportOpen && (
                <div className="absolute right-0 top-full mt-1 w-44 bg-white border border-secondary-200 rounded-md shadow-lg z-20">
                  <button
                    className="w-full text-left px-4 py-2.5 text-sm text-secondary-700 hover:bg-secondary-50 rounded-t-md"
                    onClick={() => handleExport('csv')}
                  >
                    Download CSV
                  </button>
                  <button
                    className="w-full text-left px-4 py-2.5 text-sm text-secondary-700 hover:bg-secondary-50 rounded-b-md border-t border-secondary-100"
                    onClick={() => handleExport('excel')}
                  >
                    Download Excel
                  </button>
                </div>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Results summary */}
      <div className="flex items-center gap-2 flex-wrap text-sm text-secondary-500">
        {isFetching && !isLoading ? (
          <span className="text-secondary-400 text-xs">Updating…</span>
        ) : (
          <>
            <span>
              Showing{' '}
              <span className="font-semibold text-secondary-800">{filteredTotal.toLocaleString()}</span>
              {globalTotal !== filteredTotal && (
                <> of <span className="font-medium text-secondary-600">{globalTotal.toLocaleString()}</span></>
              )}{' '}
              holdings
            </span>
            {activeFilterLabels.length > 0 && (
              <>
                <span className="text-secondary-300">—</span>
                <span className="text-secondary-400">filtered by</span>
                {activeFilterLabels.map((label, i) => (
                  <span
                    key={i}
                    className="inline-flex items-center rounded-full bg-primary-50 text-primary-700 border border-primary-100 px-2 py-0.5 text-xs font-medium"
                  >
                    {label}
                  </span>
                ))}
              </>
            )}
          </>
        )}
      </div>

      {/* Table */}
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <SortableHeader label="Company"  field="company_name"      sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} />
                <SortableHeader label="Fund"     field="fund_name"         sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} />
                <SortableHeader label="Sector"   field="sector"            sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} />
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Industry</th>
                <SortableHeader label="Country"  field="country"           sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} />
                <SortableHeader label="Value"    field="reported_value_usd" sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} align="right" />
                <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">% Fund</th>
                <SortableHeader label="As of Date" field="as_of_date"      sortBy={filters.sortBy} sortDir={filters.sortDir} onSort={handleSort} />
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                <SkeletonRows count={12} cols={8} />
              ) : !data?.items?.length ? (
                <EmptyState cols={8} />
              ) : (
                data.items.map((row) => (
                  <tr
                    key={row.holding_id}
                    className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors"
                  >
                    {/* Company */}
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
                        <span className="font-semibold text-secondary-800 truncate block" title={row.company_name ?? '—'}>
                          {row.company_name ?? '—'}
                        </span>
                      )}
                    </td>

                    {/* Fund */}
                    <td className="py-3 px-4 max-w-[180px]">
                      {row.fund_id ? (
                        <Link
                          to={`/funds/${row.fund_id}`}
                          className="text-secondary-700 hover:text-primary-700 hover:underline truncate block text-xs font-medium"
                          title={row.fund_name ?? '—'}
                        >
                          {row.fund_name ?? '—'}
                        </Link>
                      ) : (
                        <span className="text-secondary-600 truncate block text-xs">{row.fund_name ?? '—'}</span>
                      )}
                    </td>

                    {/* Sector chip */}
                    <td className="py-3 px-4">
                      <span
                        className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${sectorChipClasses(row.sector)}`}
                      >
                        {row.sector}
                      </span>
                    </td>

                    {/* Industry */}
                    <td className="py-3 px-4 text-secondary-600 max-w-[160px] truncate text-xs" title={row.industry}>
                      {row.industry && row.industry !== 'Unclassified'
                        ? row.industry
                        : <span className="text-secondary-300">—</span>}
                    </td>

                    {/* Country */}
                    <td className="py-3 px-4 text-secondary-600 text-xs">
                      {row.country && row.country !== 'Unknown'
                        ? row.country
                        : <span className="text-secondary-300">—</span>}
                    </td>

                    {/* Reported value */}
                    <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                      {row.reported_value_usd != null
                        ? formatCurrency(row.reported_value_usd)
                        : <span className="text-secondary-300">—</span>}
                    </td>

                    {/* % of fund */}
                    <td className="py-3 px-4 text-right tabular-nums text-secondary-500 text-xs">
                      {row.pct_of_fund != null
                        ? `${row.pct_of_fund.toFixed(2)}%`
                        : <span className="text-secondary-300">—</span>}
                    </td>

                    {/* As of date */}
                    <td className="py-3 px-4 text-secondary-500 text-xs">
                      {row.as_of_date ?? '—'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

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

import { useState, useCallback, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Search,
  ChevronDown,
  ChevronRight,
  AlertCircle,
  ScrollText,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
} from 'lucide-react'
import { Card, CardContent } from '../../components/ui/card'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ACTION_OPTIONS = [
  { value: '', label: 'All Actions' },
  { value: 'ai_classification', label: 'AI Classification' },
  { value: 'entity_resolution', label: 'Entity Resolution' },
  { value: 'review_queue_insert', label: 'Review Queue Insert' },
  { value: 'pipeline_run_complete', label: 'Pipeline Run Complete' },
]

const ACTION_COLORS = {
  ai_classification: 'bg-purple-100 text-purple-700',
  entity_resolution: 'bg-blue-100 text-blue-700',
  review_queue_insert: 'bg-yellow-100 text-yellow-700',
  pipeline_run_complete: 'bg-green-100 text-green-700',
}

const DATE_OPTIONS = [
  { value: '1', label: 'Today' },
  { value: '7', label: 'Last 7 days' },
  { value: '30', label: 'Last 30 days' },
  { value: '', label: 'All time' },
]

const DEFAULT_FILTERS = {
  action: '',
  days: '',
  entityId: '',
  page: 1,
  pageSize: 100,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function buildAuditUrl(filters) {
  const p = new URLSearchParams()
  p.set('page', filters.page)
  p.set('page_size', filters.pageSize)
  if (filters.action) p.set('action', filters.action)
  if (filters.days) p.set('days', filters.days)
  if (filters.entityId) p.set('entity_id', filters.entityId)
  return `/api/audit-trail?${p.toString()}`
}

function formatTimestamp(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    })
  } catch {
    return ts
  }
}

function prettyJSON(raw) {
  if (!raw) return 'No payload'
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ActionChip({ action }) {
  const cls = ACTION_COLORS[action] || 'bg-gray-100 text-gray-600'
  const label = ACTION_OPTIONS.find((o) => o.value === action)?.label ?? action
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${cls}`}>
      {label}
    </span>
  )
}

function ErrorBanner({ message }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <AlertCircle className="h-4 w-4 shrink-0" />
      <span>{message}</span>
    </div>
  )
}

function Pagination({ page, totalPages, onPageChange }) {
  if (totalPages <= 1) return null
  return (
    <div className="flex items-center justify-between px-4 py-3 border-t border-secondary-200">
      <span className="text-sm text-secondary-500">
        Page {page} of {totalPages}
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

export default function AuditTrailPage() {
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [entityIdInput, setEntityIdInput] = useState('')
  const [expandedId, setExpandedId] = useState(null)

  const setFilter = useCallback((key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }))
  }, [])

  // Debounce entity_id search
  const handleEntityIdChange = useCallback(
    (e) => {
      const val = e.target.value
      setEntityIdInput(val)
      const timer = setTimeout(() => {
        setFilter('entityId', val)
      }, 300)
      return () => clearTimeout(timer)
    },
    [setFilter]
  )

  const hasActiveFilters = filters.action !== '' || filters.days !== '' || filters.entityId !== ''

  const clearFilters = useCallback(() => {
    setEntityIdInput('')
    setFilters(DEFAULT_FILTERS)
  }, [])

  const { data, isLoading, error } = useQuery({
    queryKey: ['audit-trail', filters.page, filters.pageSize, filters.action, filters.days, filters.entityId],
    queryFn: () => fetchJSON(buildAuditUrl(filters)),
    staleTime: 30 * 1000,
    placeholderData: (prev) => prev,
  })

  const items = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / filters.pageSize))

  function toggleExpand(id) {
    setExpandedId((prev) => (prev === id ? null : id))
  }

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Audit Trail</h1>
        <p className="text-secondary-500 mt-1">Append-only event log · read-only</p>
      </div>

      {error && <ErrorBanner message={`Failed to load audit trail: ${error.message}`} />}

      {/* Filter bar */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Action dropdown */}
            <select
              value={filters.action}
              onChange={(e) => setFilter('action', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              {ACTION_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>

            {/* Date range dropdown */}
            <select
              value={filters.days}
              onChange={(e) => setFilter('days', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              {DATE_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>

            {/* Entity ID search */}
            <div className="relative min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-secondary-400" />
              <input
                type="text"
                placeholder="Search entity ID…"
                value={entityIdInput}
                onChange={handleEntityIdChange}
                className="w-full pl-9 pr-3 py-2 text-sm border border-secondary-200 rounded-md bg-white text-secondary-900 placeholder-secondary-400 focus:outline-none focus:ring-2 focus:ring-primary-500"
              />
            </div>

            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1.5 px-3 py-2 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
              >
                <span className="text-xs">✕</span>
                Clear
              </button>
            )}

            <span className="ml-auto text-sm text-secondary-500">
              {total.toLocaleString()} events
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
                <th className="w-6 py-3 px-3" />
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide whitespace-nowrap">
                  Timestamp
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Action
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Actor
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Entity Type
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Entity ID
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Payload
                </th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                [...Array(12)].map((_, i) => (
                  <tr key={i} className="border-b border-secondary-100">
                    <td className="py-3 px-3" />
                    <td className="py-3 px-3">
                      <div className="h-3 w-36 bg-secondary-200 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-5 w-32 bg-secondary-200 rounded-full animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-20 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-24 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-20 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-48 bg-secondary-100 rounded animate-pulse" />
                    </td>
                  </tr>
                ))
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-16 text-center">
                    <ScrollText className="h-10 w-10 text-secondary-200 mx-auto mb-3" />
                    <p className="text-secondary-400 text-sm">No audit events match your filters.</p>
                    {hasActiveFilters && (
                      <button
                        onClick={clearFilters}
                        className="mt-2 text-xs text-primary-600 hover:underline"
                      >
                        Clear filters
                      </button>
                    )}
                  </td>
                </tr>
              ) : (
                items.map((item) => {
                  const isExpanded = expandedId === item.audit_event_id
                  const payloadPreview = item.payload_json
                    ? item.payload_json.slice(0, 80) + (item.payload_json.length > 80 ? '…' : '')
                    : '—'

                  return (
                    <Fragment key={item.audit_event_id}>
                      <tr
                        onClick={() => toggleExpand(item.audit_event_id)}
                        className="border-b border-secondary-100 hover:bg-secondary-50 cursor-pointer transition-colors"
                      >
                        <td className="py-3 px-3 text-secondary-400">
                          {isExpanded ? (
                            <ChevronDown className="h-4 w-4" />
                          ) : (
                            <ChevronRight className="h-4 w-4" />
                          )}
                        </td>
                        <td className="py-3 px-3 text-secondary-600 text-xs tabular-nums whitespace-nowrap">
                          {formatTimestamp(item.event_time)}
                        </td>
                        <td className="py-3 px-3">
                          <ActionChip action={item.action} />
                        </td>
                        <td className="py-3 px-3 text-secondary-600 text-xs">
                          <span className="font-medium">{item.actor_type}</span>
                          {item.actor_id && item.actor_id !== item.actor_type && (
                            <span className="text-secondary-400 ml-1">· {item.actor_id.slice(0, 20)}</span>
                          )}
                        </td>
                        <td className="py-3 px-3 text-secondary-600 text-xs">{item.entity_type ?? '—'}</td>
                        <td className="py-3 px-3 font-mono text-xs text-secondary-500 max-w-[120px]">
                          <span className="truncate block" title={item.entity_id}>
                            {item.entity_id ? item.entity_id.slice(0, 16) + (item.entity_id.length > 16 ? '…' : '') : '—'}
                          </span>
                        </td>
                        <td className="py-3 px-3 text-secondary-400 text-xs max-w-[240px]">
                          <span className="truncate block" title={item.payload_json}>
                            {payloadPreview}
                          </span>
                        </td>
                      </tr>

                      {isExpanded && (
                        <tr className="border-b border-secondary-200 bg-secondary-50">
                          <td colSpan={7} className="px-6 py-4">
                            <div className="flex items-start gap-3">
                              <div className="flex-1">
                                <p className="text-xs font-semibold text-secondary-500 uppercase tracking-wide mb-2">
                                  Full Payload
                                </p>
                                <pre className="text-xs text-secondary-700 bg-white border border-secondary-200 rounded-md p-3 overflow-auto max-h-64 font-mono">
                                  {prettyJSON(item.payload_json)}
                                </pre>
                              </div>
                              <div className="text-xs text-secondary-500 space-y-1 min-w-[180px]">
                                <p>
                                  <span className="font-medium">Event ID:</span>{' '}
                                  <span className="font-mono">{item.audit_event_id?.slice(0, 8)}…</span>
                                </p>
                                <p>
                                  <span className="font-medium">Run ID:</span>{' '}
                                  <span className="font-mono">{item.run_id?.slice(0, 8)}…</span>
                                </p>
                                <p>
                                  <span className="font-medium">Actor type:</span> {item.actor_type}
                                </p>
                                <p>
                                  <span className="font-medium">Actor ID:</span> {item.actor_id ?? '—'}
                                </p>
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })
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

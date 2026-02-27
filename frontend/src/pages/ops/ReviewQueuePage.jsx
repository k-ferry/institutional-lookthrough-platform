import { useState, useCallback } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  X,
  CheckCircle,
  XCircle,
  MinusCircle,
  AlertCircle,
  ClipboardList,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from 'lucide-react'
import { Card, CardContent } from '../../components/ui/card'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REASON_LABELS = {
  unresolved_entity: 'Unresolved Entity',
  low_confidence_classification: 'Low Confidence',
  unclassifiable_company: 'Cannot Classify',
  large_unknown_exposure: 'Large Unknown Exposure',
}

const REASON_OPTIONS = [
  { value: '', label: 'All Reasons' },
  { value: 'unresolved_entity', label: 'Unresolved Entity' },
  { value: 'low_confidence_classification', label: 'Low Confidence' },
  { value: 'unclassifiable_company', label: 'Cannot Classify' },
  { value: 'large_unknown_exposure', label: 'Large Unknown Exposure' },
]

const DEFAULT_FILTERS = {
  status: 'pending',
  priority: 'all',
  reason: '',
  page: 1,
  pageSize: 50,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function buildQueueUrl(filters) {
  const p = new URLSearchParams()
  p.set('page', filters.page)
  p.set('page_size', filters.pageSize)
  p.set('status', filters.status)
  if (filters.priority !== 'all') p.set('priority', filters.priority)
  if (filters.reason) p.set('reason', filters.reason)
  return `/api/review-queue?${p.toString()}`
}

function formatTimestamp(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return ts
  }
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function PriorityBadge({ priority }) {
  const cls =
    { high: 'bg-red-100 text-red-700', medium: 'bg-yellow-100 text-yellow-700', low: 'bg-gray-100 text-gray-600' }[
      priority
    ] || 'bg-gray-100 text-gray-600'
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold capitalize ${cls}`}>
      {priority}
    </span>
  )
}

function StatusBadge({ status }) {
  const cls =
    {
      pending: 'bg-blue-50 text-blue-700 border border-blue-200',
      approved: 'bg-green-50 text-green-700 border border-green-200',
      rejected: 'bg-red-50 text-red-700 border border-red-200',
      dismissed: 'bg-gray-100 text-gray-500 border border-gray-200',
    }[status] || 'bg-gray-100 text-gray-500 border border-gray-200'
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${cls}`}>
      {status}
    </span>
  )
}

function StatCard({ title, value, description, colorClass }) {
  return (
    <Card>
      <CardContent className="p-4">
        <p className="text-xs font-medium text-secondary-500 uppercase tracking-wide">{title}</p>
        <p className={`text-2xl font-bold mt-1 ${colorClass || 'text-secondary-900'}`}>{value ?? '—'}</p>
        {description && <p className="text-xs text-secondary-400 mt-0.5">{description}</p>}
      </CardContent>
    </Card>
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

export default function ReviewQueuePage() {
  const queryClient = useQueryClient()
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [selectedIds, setSelectedIds] = useState(new Set())
  const [actionLoading, setActionLoading] = useState(null)
  const [actionError, setActionError] = useState(null)

  const setFilter = useCallback((key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }))
    setSelectedIds(new Set())
  }, [])

  const clearFilters = useCallback(() => {
    setFilters(DEFAULT_FILTERS)
    setSelectedIds(new Set())
  }, [])

  const hasActiveFilters =
    filters.status !== 'pending' || filters.priority !== 'all' || filters.reason !== ''

  const { data: stats } = useQuery({
    queryKey: ['review-queue-stats'],
    queryFn: () => fetchJSON('/api/review-queue/stats'),
    staleTime: 30 * 1000,
  })

  const { data, isLoading, error } = useQuery({
    queryKey: ['review-queue', filters.page, filters.pageSize, filters.status, filters.priority, filters.reason],
    queryFn: () => fetchJSON(buildQueueUrl(filters)),
    staleTime: 30 * 1000,
    placeholderData: (prev) => prev,
  })

  const items = data?.items ?? []
  const total = data?.total ?? 0
  const counts = data?.counts ?? {}
  const totalPages = Math.max(1, Math.ceil(total / filters.pageSize))
  const allSelected = items.length > 0 && selectedIds.size === items.length

  async function handleAction(itemId, status) {
    setActionLoading(itemId)
    setActionError(null)
    try {
      const res = await fetch(`/api/review-queue/${itemId}`, {
        method: 'PATCH',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status }),
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      queryClient.invalidateQueries({ queryKey: ['review-queue'] })
      queryClient.invalidateQueries({ queryKey: ['review-queue-stats'] })
    } catch (e) {
      setActionError(`Action failed: ${e.message}`)
    } finally {
      setActionLoading(null)
    }
  }

  async function handleBulkAction(status) {
    if (selectedIds.size === 0) return
    setActionLoading('bulk')
    setActionError(null)
    try {
      const res = await fetch('/api/review-queue/bulk', {
        method: 'PATCH',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ item_ids: [...selectedIds], status }),
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      setSelectedIds(new Set())
      queryClient.invalidateQueries({ queryKey: ['review-queue'] })
      queryClient.invalidateQueries({ queryKey: ['review-queue-stats'] })
    } catch (e) {
      setActionError(`Bulk action failed: ${e.message}`)
    } finally {
      setActionLoading(null)
    }
  }

  function toggleSelect(id) {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function toggleSelectAll() {
    if (allSelected) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(items.map((i) => i.queue_item_id)))
    }
  }

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Review Queue</h1>
        <p className="text-secondary-500 mt-1">
          {stats
            ? `${(stats.pending ?? 0).toLocaleString()} pending · ${(stats.high_priority ?? 0).toLocaleString()} high priority`
            : 'Loading…'}
        </p>
      </div>

      {actionError && <ErrorBanner message={actionError} />}
      {error && <ErrorBanner message={`Failed to load queue: ${error.message}`} />}

      {/* Stats bar */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard title="Total Items" value={stats?.total?.toLocaleString()} description="All time" />
        <StatCard
          title="Pending"
          value={stats?.pending?.toLocaleString()}
          description="Awaiting review"
          colorClass="text-blue-700"
        />
        <StatCard
          title="High Priority"
          value={stats?.high_priority?.toLocaleString()}
          description="Pending only"
          colorClass="text-red-600"
        />
        <StatCard
          title="Approved Today"
          value={stats?.approved_today?.toLocaleString()}
          description="UTC day"
          colorClass="text-green-600"
        />
      </div>

      {/* Filter bar */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Status dropdown */}
            <select
              value={filters.status}
              onChange={(e) => setFilter('status', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="pending">Pending{counts.pending ? ` (${counts.pending})` : ''}</option>
              <option value="approved">Approved{counts.approved ? ` (${counts.approved})` : ''}</option>
              <option value="rejected">Rejected{counts.rejected ? ` (${counts.rejected})` : ''}</option>
              <option value="dismissed">Dismissed{counts.dismissed ? ` (${counts.dismissed})` : ''}</option>
              <option value="all">All Statuses</option>
            </select>

            {/* Priority dropdown */}
            <select
              value={filters.priority}
              onChange={(e) => setFilter('priority', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="all">All Priorities</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>

            {/* Reason dropdown */}
            <select
              value={filters.reason}
              onChange={(e) => setFilter('reason', e.target.value)}
              className="py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              {REASON_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>

            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1.5 px-3 py-2 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
              >
                <X className="h-3.5 w-3.5" />
                Clear
              </button>
            )}

            {/* Bulk actions — visible only when items are selected */}
            {selectedIds.size > 0 ? (
              <div className="ml-auto flex items-center gap-2">
                <span className="text-sm text-secondary-600 mr-1">
                  {selectedIds.size} selected
                </span>
                <button
                  onClick={() => handleBulkAction('approved')}
                  disabled={actionLoading === 'bulk'}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-50 transition-colors"
                >
                  <CheckCircle className="h-3.5 w-3.5" />
                  Approve Selected
                </button>
                <button
                  onClick={() => handleBulkAction('dismissed')}
                  disabled={actionLoading === 'bulk'}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white bg-gray-500 rounded-md hover:bg-gray-600 disabled:opacity-50 transition-colors"
                >
                  <MinusCircle className="h-3.5 w-3.5" />
                  Dismiss Selected
                </button>
              </div>
            ) : (
              <span className="ml-auto text-sm text-secondary-500">
                {total.toLocaleString()} items
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <th className="py-3 px-4 w-10">
                  <input
                    type="checkbox"
                    checked={allSelected}
                    onChange={toggleSelectAll}
                    className="h-4 w-4 rounded border-secondary-300 text-primary-600 focus:ring-primary-500"
                  />
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Priority
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Company
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Reason
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Status
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Created
                </th>
                <th className="text-left py-3 px-3 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                [...Array(10)].map((_, i) => (
                  <tr key={i} className="border-b border-secondary-100">
                    <td className="py-3 px-4">
                      <div className="h-4 w-4 bg-secondary-200 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-5 w-14 bg-secondary-200 rounded-full animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3.5 w-44 bg-secondary-200 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-36 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-5 w-16 bg-secondary-100 rounded-full animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-3 w-24 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-3">
                      <div className="h-6 w-52 bg-secondary-100 rounded animate-pulse" />
                    </td>
                  </tr>
                ))
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-16 text-center">
                    <ClipboardList className="h-10 w-10 text-secondary-200 mx-auto mb-3" />
                    <p className="text-secondary-400 text-sm">No items match your filters.</p>
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
                  const isSelected = selectedIds.has(item.queue_item_id)
                  const isProcessing = actionLoading === item.queue_item_id
                  return (
                    <tr
                      key={item.queue_item_id}
                      className={`border-b border-secondary-100 transition-colors ${
                        isSelected ? 'bg-primary-50' : 'hover:bg-secondary-50'
                      }`}
                    >
                      <td className="py-3 px-4">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleSelect(item.queue_item_id)}
                          className="h-4 w-4 rounded border-secondary-300 text-primary-600 focus:ring-primary-500"
                        />
                      </td>
                      <td className="py-3 px-3">
                        <PriorityBadge priority={item.priority} />
                      </td>
                      <td className="py-3 px-3 max-w-[200px]">
                        <span
                          className="font-medium text-secondary-900 truncate block"
                          title={item.company_name}
                        >
                          {item.company_name ?? '—'}
                        </span>
                        {item.primary_sector && (
                          <span className="text-xs text-secondary-400">{item.primary_sector}</span>
                        )}
                      </td>
                      <td className="py-3 px-3 text-secondary-600 text-xs whitespace-nowrap">
                        {REASON_LABELS[item.reason] ?? item.reason}
                      </td>
                      <td className="py-3 px-3">
                        <StatusBadge status={item.status} />
                      </td>
                      <td className="py-3 px-3 text-secondary-500 text-xs tabular-nums whitespace-nowrap">
                        {formatTimestamp(item.created_at)}
                      </td>
                      <td className="py-3 px-3">
                        <div className="flex items-center gap-1.5">
                          <button
                            onClick={() => handleAction(item.queue_item_id, 'approved')}
                            disabled={isProcessing || item.status === 'approved'}
                            className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-white bg-green-600 rounded hover:bg-green-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                          >
                            <CheckCircle className="h-3.5 w-3.5" />
                            Approve
                          </button>
                          <button
                            onClick={() => handleAction(item.queue_item_id, 'rejected')}
                            disabled={isProcessing || item.status === 'rejected'}
                            className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-white bg-red-500 rounded hover:bg-red-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                          >
                            <XCircle className="h-3.5 w-3.5" />
                            Reject
                          </button>
                          <button
                            onClick={() => handleAction(item.queue_item_id, 'dismissed')}
                            disabled={isProcessing || item.status === 'dismissed'}
                            className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-secondary-600 bg-secondary-100 rounded hover:bg-secondary-200 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                          >
                            <MinusCircle className="h-3.5 w-3.5" />
                            Dismiss
                          </button>
                        </div>
                      </td>
                    </tr>
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

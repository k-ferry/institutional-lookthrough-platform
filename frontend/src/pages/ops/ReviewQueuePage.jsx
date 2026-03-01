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
  Info,
  Sparkles,
  Loader2,
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

const REASON_TOOLTIPS = {
  unresolved_entity:
    'Company name could not be matched to any known entity after 5 resolution strategies',
  low_confidence_classification:
    'AI classification confidence is below 0.70 — manual verification recommended',
  unclassifiable_company:
    'AI attempted classification but confidence was below threshold or returned null',
  large_unknown_exposure:
    'Exposure value exceeds $1M but company has no sector classification',
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

function formatCurrency(value) {
  if (value == null) return 'N/A'
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `$${(value / 1_000).toFixed(0)}K`
  return `$${value.toFixed(0)}`
}

function confidenceColorClass(score) {
  if (score == null) return 'text-secondary-400'
  if (score >= 0.9) return 'text-green-600'
  if (score >= 0.75) return 'text-yellow-600'
  return 'text-red-600'
}

function confidenceBadgeClass(score) {
  if (score == null) return 'bg-secondary-100 text-secondary-500'
  if (score >= 0.9) return 'bg-green-100 text-green-700'
  if (score >= 0.75) return 'bg-yellow-100 text-yellow-700'
  return 'bg-red-100 text-red-700'
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

// Reason badge with hover tooltip
function ReasonBadge({ reason }) {
  const label = REASON_LABELS[reason] ?? reason
  const tooltip = REASON_TOOLTIPS[reason]
  return (
    <span className="relative group inline-flex items-center gap-1 cursor-default">
      <span className="text-secondary-600 text-xs">{label}</span>
      {tooltip && (
        <>
          <Info className="h-3 w-3 text-secondary-400 flex-shrink-0" />
          <span className="absolute left-0 bottom-full mb-2 z-20 w-72 px-3 py-2 text-xs text-white bg-secondary-800 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity duration-150 pointer-events-none leading-relaxed">
            {tooltip}
          </span>
        </>
      )}
    </span>
  )
}

// Confidence score as a colored percentage badge
function ConfidenceBadge({ score }) {
  if (score == null) return <span className="text-secondary-400 text-xs">—</span>
  const pct = Math.round(score * 100)
  return (
    <span className={`inline-flex items-center rounded px-1.5 py-0.5 text-xs font-semibold ${confidenceBadgeClass(score)}`}>
      {pct}%
    </span>
  )
}

const NULL_UUID = '00000000-0000-0000-0000-000000000000'

function ProviderBadge({ provider }) {
  const config = {
    claude: { label: 'Claude', cls: 'bg-amber-100 text-amber-700' },
    openai: { label: 'GPT-4o', cls: 'bg-green-100 text-green-700' },
    ollama: { label: 'Llama3.1', cls: 'bg-purple-100 text-purple-700' },
  }[provider] || { label: provider, cls: 'bg-secondary-100 text-secondary-600' }
  return (
    <span className={`inline-flex items-center rounded px-2 py-0.5 text-xs font-semibold ${config.cls}`}>
      {config.label}
    </span>
  )
}

// Expandable detail panel rendered as a full-width row below the item row
function DetailPanel({ item, onAction, actionLoading }) {
  const [notes, setNotes] = useState('')
  const [selectedProvider, setSelectedProvider] = useState('claude')
  const [researchLoading, setResearchLoading] = useState(false)
  const [researchResult, setResearchResult] = useState(null)
  const ai = item.ai_classification
  const isProcessing = actionLoading === item.queue_item_id

  async function handleResearch() {
    setResearchLoading(true)
    setResearchResult(null)
    try {
      const res = await fetch('/api/review-queue/research', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          company_name: item.company_name,
          company_id: item.company_id,
          raw_company_name: item.raw_company_name,
          reported_sector: item.reported_sector,
          provider: selectedProvider,
        }),
      })
      const data = await res.json()
      setResearchResult(data)
    } catch (e) {
      setResearchResult({ error: `Request failed: ${e.message}` })
    } finally {
      setResearchLoading(false)
    }
  }

  return (
    <div className="bg-secondary-50 border-t border-secondary-200 px-6 py-5">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* LEFT — Company Context */}
        <div className="space-y-4">

          {/* Name */}
          <div>
            <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-1">Company</p>
            <p className="text-lg font-bold text-secondary-900 leading-tight">{item.company_name ?? '—'}</p>
            {item.raw_company_name && item.raw_company_name !== item.company_name && (
              <p className="text-xs text-secondary-400 mt-0.5">Raw: {item.raw_company_name}</p>
            )}
          </div>

          {/* Fund badges */}
          {item.fund_names?.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-1.5">Funds</p>
              <div className="flex flex-wrap gap-1.5">
                {item.fund_names.map((name) => (
                  <span
                    key={name}
                    className="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium bg-primary-50 text-primary-700 border border-primary-200"
                  >
                    {name}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Holdings summary cards */}
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg border border-secondary-200 bg-white px-3 py-2.5">
              <p className="text-xs text-secondary-400">Holdings</p>
              <p className="text-sm font-semibold text-secondary-900 mt-0.5">
                {item.holding_count != null ? item.holding_count.toLocaleString() : 'N/A'}
              </p>
            </div>
            <div className="rounded-lg border border-secondary-200 bg-white px-3 py-2.5">
              <p className="text-xs text-secondary-400">Total Value</p>
              <p className="text-sm font-semibold text-secondary-900 mt-0.5">
                {formatCurrency(item.reported_value_usd)}
              </p>
            </div>
          </div>

          {/* Filed sector */}
          <div>
            <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-1">Filed As</p>
            {item.reported_sector ? (
              <p className="text-sm text-secondary-700">{item.reported_sector}</p>
            ) : (
              <p className="text-sm text-secondary-400 italic">No sector in filing</p>
            )}
          </div>

          {/* Entity resolution */}
          <div>
            <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-1.5">
              Entity Resolution
            </p>
            {item.match_method ? (
              <div className="flex items-center gap-3">
                <span className="text-xs text-secondary-600 bg-secondary-100 rounded px-2 py-0.5 font-mono">
                  {item.match_method.replace(/_/g, ' ')}
                </span>
                <span className={`text-sm font-semibold ${confidenceColorClass(item.match_confidence)}`}>
                  {item.match_confidence != null
                    ? `${Math.round(item.match_confidence * 100)}%`
                    : '—'}
                </span>
              </div>
            ) : (
              <p className="text-xs text-secondary-400 italic">No match recorded</p>
            )}
          </div>
        </div>

        {/* RIGHT — AI Classification */}
        <div className="space-y-3">
          <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide">AI Classification</p>
          {ai ? (
            <div className="rounded-lg border border-secondary-200 bg-white p-4 space-y-3">
              <div className="flex items-start justify-between gap-3">
                <p className="text-sm font-semibold leading-snug">
                  {ai.node_name ? (
                    <span className="text-secondary-900">{ai.node_name}</span>
                  ) : !ai.taxonomy_node_id || ai.taxonomy_node_id === NULL_UUID ? (
                    <span className="text-red-400">Could not classify</span>
                  ) : (
                    <span className="text-secondary-900">{ai.taxonomy_node_id}</span>
                  )}
                </p>
                <ConfidenceBadge score={ai.confidence} />
              </div>
              {ai.rationale && (
                <div>
                  <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-1">
                    Rationale
                  </p>
                  <p className="text-xs text-secondary-600 leading-relaxed">{ai.rationale}</p>
                </div>
              )}
              {ai.model && (
                <p className="text-xs text-secondary-400 font-mono mt-1">{ai.model}</p>
              )}
            </div>
          ) : (
            <div className="rounded-lg border border-secondary-200 bg-white p-4">
              <p className="text-sm text-secondary-400 italic">AI could not classify this company</p>
            </div>
          )}
        </div>
      </div>

      {/* RESEARCH — LLM research panel */}
      <div className="mt-5 pt-4 border-t border-secondary-200">
        <p className="text-xs font-semibold text-secondary-400 uppercase tracking-wide mb-3">
          Research this Company
        </p>

        {/* LLM selector */}
        <div className="flex flex-wrap items-center gap-2 mb-3">
          {[
            {
              id: 'claude',
              label: 'Claude',
              activeClass: 'bg-amber-500 border-amber-500 text-white',
              hoverClass: 'hover:border-amber-400 hover:text-amber-600',
            },
            {
              id: 'openai',
              label: 'GPT-4o',
              activeClass: 'bg-green-600 border-green-600 text-white',
              hoverClass: 'hover:border-green-500 hover:text-green-600',
            },
            {
              id: 'ollama',
              label: 'Llama3.1 — Local',
              activeClass: 'bg-purple-600 border-purple-600 text-white',
              hoverClass: 'hover:border-purple-500 hover:text-purple-600',
              badge: true,
            },
          ].map(({ id, label, activeClass, hoverClass, badge }) => (
            <button
              key={id}
              onClick={() => setSelectedProvider(id)}
              className={`flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-md border transition-colors ${
                selectedProvider === id
                  ? activeClass
                  : `bg-white text-secondary-600 border-secondary-200 ${hoverClass}`
              }`}
            >
              {label}
              {badge && (
                <span
                  className={`ml-1 text-[10px] px-1 rounded ${
                    selectedProvider === id ? 'bg-white/25 text-white' : 'bg-purple-100 text-purple-700'
                  }`}
                >
                  free
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Research button */}
        <button
          onClick={handleResearch}
          disabled={researchLoading}
          className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-slate-800 rounded-md hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {researchLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Sparkles className="h-4 w-4" />
          )}
          {researchLoading
            ? selectedProvider === 'ollama'
              ? 'Researching… (local models may take 30–60s)'
              : 'Researching…'
            : 'Research Company'}
        </button>

        {/* Response area */}
        {researchResult && (
          <div className="mt-4 rounded-lg border border-secondary-200 bg-white p-4">
            {researchResult.error ? (
              <p className="text-sm text-red-600">{researchResult.error}</p>
            ) : (
              <>
                <div className="flex items-center gap-3 mb-3">
                  <ProviderBadge provider={researchResult.provider} />
                  <span className="text-xs text-secondary-400">{researchResult.duration_ms}ms</span>
                  <button
                    onClick={() => setResearchResult(null)}
                    className="ml-auto text-xs text-secondary-400 hover:text-secondary-600 underline"
                  >
                    Try different LLM
                  </button>
                </div>
                <div className="text-sm text-secondary-700 leading-relaxed whitespace-pre-wrap">
                  {researchResult.response}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* BOTTOM — Reviewer notes + actions */}
      <div className="mt-5 pt-4 border-t border-secondary-200">
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
          <input
            type="text"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Add reviewer notes (optional)…"
            className="flex-1 min-w-0 py-2 px-3 text-sm border border-secondary-200 rounded-md bg-white text-secondary-700 placeholder:text-secondary-400 focus:outline-none focus:ring-2 focus:ring-primary-500"
          />
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => onAction(item.queue_item_id, 'approved', notes)}
              disabled={isProcessing || item.status === 'approved'}
              className="flex items-center gap-1 px-3 py-2 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              <CheckCircle className="h-4 w-4" />
              Approve
            </button>
            <button
              onClick={() => onAction(item.queue_item_id, 'rejected', notes)}
              disabled={isProcessing || item.status === 'rejected'}
              className="flex items-center gap-1 px-3 py-2 text-sm font-medium text-white bg-red-500 rounded-md hover:bg-red-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              <XCircle className="h-4 w-4" />
              Reject
            </button>
            <button
              onClick={() => onAction(item.queue_item_id, 'dismissed', notes)}
              disabled={isProcessing || item.status === 'dismissed'}
              className="flex items-center gap-1 px-3 py-2 text-sm font-medium text-secondary-600 bg-secondary-100 rounded-md hover:bg-secondary-200 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              <MinusCircle className="h-4 w-4" />
              Dismiss
            </button>
          </div>
        </div>
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
  const [expandedId, setExpandedId] = useState(null)

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

  // notes param is passed from the detail panel; inline buttons omit it
  async function handleAction(itemId, status, notes = null) {
    setActionLoading(itemId)
    setActionError(null)
    try {
      const res = await fetch(`/api/review-queue/${itemId}`, {
        method: 'PATCH',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          status,
          ...(notes ? { reviewer_notes: notes } : {}),
        }),
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      setExpandedId(null)
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

  function toggleExpand(id) {
    setExpandedId((prev) => (prev === id ? null : id))
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
                <span className="text-sm text-secondary-600 mr-1">{selectedIds.size} selected</span>
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
              <span className="ml-auto text-sm text-secondary-500">{total.toLocaleString()} items</span>
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
                items.flatMap((item) => {
                  const isSelected = selectedIds.has(item.queue_item_id)
                  const isProcessing = actionLoading === item.queue_item_id
                  const isExpanded = expandedId === item.queue_item_id

                  return [
                    // Main row — clicking expands; checkbox and actions cells stop propagation
                    <tr
                      key={item.queue_item_id}
                      onClick={() => toggleExpand(item.queue_item_id)}
                      className={`border-b border-secondary-100 cursor-pointer transition-colors ${
                        isExpanded
                          ? 'bg-primary-50 border-b-0'
                          : isSelected
                          ? 'bg-primary-50'
                          : 'hover:bg-secondary-50'
                      }`}
                    >
                      <td className="py-3 px-4" onClick={(e) => e.stopPropagation()}>
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
                      <td className="py-3 px-3">
                        <ReasonBadge reason={item.reason} />
                      </td>
                      <td className="py-3 px-3">
                        <StatusBadge status={item.status} />
                      </td>
                      <td className="py-3 px-3 text-secondary-500 text-xs tabular-nums whitespace-nowrap">
                        {formatTimestamp(item.created_at)}
                      </td>
                      <td className="py-3 px-3" onClick={(e) => e.stopPropagation()}>
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
                    </tr>,

                    // Detail panel row — rendered only when this row is expanded
                    isExpanded && (
                      <tr key={`${item.queue_item_id}-detail`} className="border-b border-secondary-200">
                        <td colSpan={7} className="p-0">
                          <DetailPanel
                            item={item}
                            onAction={handleAction}
                            actionLoading={actionLoading}
                          />
                        </td>
                      </tr>
                    ),
                  ].filter(Boolean)
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

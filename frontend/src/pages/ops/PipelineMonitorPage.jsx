import { useState, useEffect, useRef, useCallback } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertCircle,
  Database,
  CheckCircle2,
  Activity,
  ClipboardList,
  Layers,
  FileText,
  Play,
  RefreshCw,
  ChevronDown,
  ChevronUp,
  Loader2,
  CheckCircle,
  XCircle,
  Info,
  Terminal,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

async function postJSON(url, body = {}) {
  const res = await fetch(url, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail ?? `${res.status} ${res.statusText}`)
  }
  return res.json()
}

function formatNumber(n) {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
}

function formatTimestamp(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return ts }
}

function formatRelativeDate(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  } catch { return ts }
}

// ---------------------------------------------------------------------------
// Shared sub-components (existing)
// ---------------------------------------------------------------------------

function ErrorBanner({ message }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <AlertCircle className="h-4 w-4 shrink-0" />
      <span>{message}</span>
    </div>
  )
}

function StatCard({ title, value, description, icon: Icon, colorClass }) {
  return (
    <Card>
      <CardContent className="p-5">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-xs font-medium text-secondary-500 uppercase tracking-wide">{title}</p>
            <p className={`text-2xl font-bold mt-1 ${colorClass || 'text-secondary-900'}`}>{value ?? '—'}</p>
            {description && <p className="text-xs text-secondary-400 mt-0.5">{description}</p>}
          </div>
          {Icon && (
            <div className="h-10 w-10 rounded-full bg-primary-50 flex items-center justify-center shrink-0">
              <Icon className="h-5 w-5 text-primary-600" />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

function StatSkeleton() {
  return (
    <Card>
      <CardContent className="p-5">
        <div className="space-y-2">
          <div className="h-3 w-24 bg-secondary-200 rounded animate-pulse" />
          <div className="h-7 w-20 bg-secondary-200 rounded animate-pulse" />
          <div className="h-2 w-32 bg-secondary-100 rounded animate-pulse" />
        </div>
      </CardContent>
    </Card>
  )
}

function ProgressBar({ value, color = 'bg-primary-600', animated = false }) {
  const pct = Math.min(100, Math.max(0, value ?? 0))
  return (
    <div className="w-full bg-secondary-100 rounded-full h-2.5 overflow-hidden">
      <div
        className={`${color} h-2.5 rounded-full transition-all duration-500 ${animated ? 'animate-pulse' : ''}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  )
}

function CoverageRow({ label, value, description }) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-secondary-700">{label}</span>
        <span className={`text-sm font-bold tabular-nums ${value >= 80 ? 'text-green-600' : value >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
          {value != null ? `${value.toFixed(1)}%` : '—'}
        </span>
      </div>
      <ProgressBar value={value} color={value >= 80 ? 'bg-green-500' : value >= 50 ? 'bg-yellow-500' : 'bg-red-500'} />
      {description && <p className="text-xs text-secondary-400">{description}</p>}
    </div>
  )
}

const SOURCE_CONFIG = {
  bdc_filing:   { label: 'BDC Filing',   color: 'bg-primary-600',   text: 'text-white' },
  pdf_document: { label: 'PDF Document', color: 'bg-emerald-600',   text: 'text-white' },
  '13f_filing': { label: '13F Filing',   color: 'bg-amber-500',     text: 'text-white' },
  synthetic:    { label: 'Synthetic',    color: 'bg-secondary-300', text: 'text-secondary-700' },
}

function SourceBreakdownBar({ holdingsBySource }) {
  const entries = Object.entries(holdingsBySource ?? {}).sort((a, b) => b[1] - a[1])
  const total = entries.reduce((s, [, v]) => s + v, 0)
  if (!total) return <p className="text-sm text-secondary-400">No holdings data</p>
  return (
    <div className="space-y-3">
      <div className="flex h-7 rounded-md overflow-hidden text-xs font-medium">
        {entries.map(([source, count]) => {
          const pct = (count / total) * 100
          if (pct === 0) return null
          const cfg = SOURCE_CONFIG[source] ?? { label: source, color: 'bg-teal-500', text: 'text-white' }
          return (
            <div
              key={source}
              className={`${cfg.color} ${cfg.text} flex items-center justify-center`}
              style={{ width: `${pct}%` }}
              title={`${cfg.label}: ${formatNumber(count)}`}
            >
              {pct > 8 ? `${pct.toFixed(0)}%` : ''}
            </div>
          )
        })}
      </div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-secondary-600">
        {entries.map(([source, count]) => {
          const cfg = SOURCE_CONFIG[source] ?? { label: source, color: 'bg-teal-500' }
          return (
            <span key={source} className="flex items-center gap-1.5">
              <span className={`h-2.5 w-2.5 rounded-sm ${cfg.color} inline-block`} />
              {cfg.label} ({formatNumber(count)})
            </span>
          )
        })}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Ingestion section sub-components
// ---------------------------------------------------------------------------

function FileStatusBadge({ status }) {
  if (status === 'ingested') return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800"><CheckCircle2 className="h-3 w-3" />Ingested</span>
  if (status === 'new') return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">New</span>
  if (status === 'changed') return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-100 text-amber-800">Changed</span>
  return null
}

function FundStatusBadge({ files }) {
  const hasNew = files.some(f => f.status === 'new')
  const hasChanged = files.some(f => f.status === 'changed')
  const hasAny = files.length > 0
  const allIngested = hasAny && files.every(f => f.status === 'ingested')

  if (!hasAny) return <span className="text-xs text-secondary-400">No files found</span>
  if (allIngested) return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800"><CheckCircle2 className="h-3 w-3" />Up to date</span>
  if (hasNew || hasChanged) return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">New files</span>
  return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">Never ingested</span>
}

function RunStatusBadge({ status }) {
  if (status === 'running') return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800"><Loader2 className="h-3 w-3 animate-spin" />Running</span>
  if (status === 'complete') return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-green-100 text-green-800"><CheckCircle className="h-3 w-3" />Complete</span>
  if (status === 'failed') return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-red-100 text-red-800"><XCircle className="h-3 w-3" />Failed</span>
  return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-gray-100 text-gray-600">Idle</span>
}

function LogViewer({ lines, onClear }) {
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines])

  function levelClass(level) {
    if (level === 'ERROR') return 'text-red-400'
    if (level === 'WARNING') return 'text-amber-400'
    return 'text-secondary-400'
  }

  return (
    <div className="rounded-lg border border-secondary-200 bg-secondary-950 overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 bg-secondary-900 border-b border-secondary-700">
        <div className="flex items-center gap-2">
          <Terminal className="h-3.5 w-3.5 text-secondary-400" />
          <span className="text-xs font-medium text-secondary-400 uppercase tracking-wide">Ingestion Log</span>
          <span className="text-xs text-secondary-600">({lines.length} lines)</span>
        </div>
        <button onClick={onClear} className="text-xs text-secondary-500 hover:text-secondary-300 transition-colors">
          Clear
        </button>
      </div>
      <div className="h-64 overflow-y-auto p-3 font-mono text-xs space-y-0.5">
        {lines.length === 0 ? (
          <p className="text-secondary-600 italic">No log output yet. Run ingestion to see output here.</p>
        ) : (
          lines.map((line, i) => (
            <div key={i} className="flex gap-2">
              <span className="text-secondary-700 shrink-0 tabular-nums">
                {line.timestamp ? new Date(line.timestamp).toLocaleTimeString('en-US', { hour12: false }) : ''}
              </span>
              <span className={`shrink-0 w-14 font-semibold ${levelClass(line.level)}`}>{line.level}</span>
              <span className="text-secondary-300 break-all">{line.message}</span>
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Document Ingestion section
// ---------------------------------------------------------------------------

function DocumentIngestionSection() {
  const queryClient = useQueryClient()
  const [selectedFunds, setSelectedFunds] = useState(new Set())
  const [confirmDialog, setConfirmDialog] = useState(null) // null | { funds: string[], fileCount: number }
  const [isTriggering, setIsTriggering] = useState(false)
  const [triggerError, setTriggerError] = useState(null)
  const [showLogs, setShowLogs] = useState(false)
  const [localLogs, setLocalLogs] = useState([])
  const [expandedFunds, setExpandedFunds] = useState({})

  // Manifest data
  const {
    data: manifest,
    isLoading: manifestLoading,
    isFetching: manifestFetching,
    error: manifestError,
    refetch: refetchManifest,
  } = useQuery({
    queryKey: ['ingestion-manifest'],
    queryFn: () => fetchJSON('/api/ingestion/manifest'),
    staleTime: 60 * 1000,
    retry: 1,
  })

  // Run status — polls every 2s while running
  const {
    data: runStatus,
    refetch: refetchStatus,
  } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: () => fetchJSON('/api/ingestion/status'),
    staleTime: 0,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      return status === 'running' ? 2000 : false
    },
  })

  const isRunning = runStatus?.status === 'running'
  const isComplete = runStatus?.status === 'complete'
  const isFailed = runStatus?.status === 'failed'
  const hasResult = isComplete || isFailed

  // When run transitions to complete/failed, refresh manifest and fetch logs
  const prevStatusRef = useRef(null)
  useEffect(() => {
    const cur = runStatus?.status
    if (prevStatusRef.current === 'running' && (cur === 'complete' || cur === 'failed')) {
      refetchManifest()
      queryClient.invalidateQueries({ queryKey: ['dashboard-stats'] })
      queryClient.invalidateQueries({ queryKey: ['pipeline-stats'] })
      fetchLogs()
    }
    prevStatusRef.current = cur
  }, [runStatus?.status])

  // Fetch logs on demand / during run
  const fetchLogs = useCallback(async () => {
    try {
      const logs = await fetchJSON('/api/ingestion/logs')
      setLocalLogs(logs)
    } catch { /* ignore */ }
  }, [])

  // Auto-fetch logs every 2s during run
  useEffect(() => {
    if (!isRunning) return
    setShowLogs(true)
    const interval = setInterval(fetchLogs, 2000)
    fetchLogs()
    return () => clearInterval(interval)
  }, [isRunning, fetchLogs])

  async function handleRun(force = false) {
    setConfirmDialog(null)
    setTriggerError(null)
    setIsTriggering(true)
    try {
      await postJSON('/api/ingestion/run', {
        force,
        fund_filters: Array.from(selectedFunds),
      })
      setShowLogs(true)
      setLocalLogs([])
      refetchStatus()
    } catch (err) {
      setTriggerError(err.message)
    } finally {
      setIsTriggering(false)
    }
  }

  function openConfirmDialog() {
    const funds = Array.from(selectedFunds)
    const fundData = manifest?.by_fund ?? []
    const fileCount = fundData
      .filter(f => funds.length === 0 || funds.includes(f.folder))
      .reduce((s, f) => s + f.file_count, 0)
    setConfirmDialog({ funds, fileCount })
  }

  function toggleFundSelection(folder) {
    setSelectedFunds(prev => {
      const next = new Set(prev)
      if (next.has(folder)) next.delete(folder)
      else next.add(folder)
      return next
    })
  }

  function selectAll() {
    setSelectedFunds(new Set((manifest?.by_fund ?? []).map(f => f.folder)))
  }

  function deselectAll() {
    setSelectedFunds(new Set())
  }

  const progress = runStatus?.progress ?? {}
  const filePct = progress.total_files > 0
    ? Math.round((progress.files_processed + progress.files_skipped) / progress.total_files * 100)
    : 0

  function toggleFund(folder) {
    setExpandedFunds(prev => ({ ...prev, [folder]: !prev[folder] }))
  }

  return (
    <div className="space-y-5">
      {/* Section header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <FileText className="h-5 w-5 text-primary-600" />
          <h2 className="text-lg font-semibold text-secondary-900">Document Ingestion</h2>
          {runStatus && <RunStatusBadge status={runStatus.status} />}
        </div>
        <button
          onClick={() => { refetchManifest(); refetchStatus() }}
          disabled={manifestFetching}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-secondary-600 border border-secondary-200 rounded-md hover:bg-secondary-50 disabled:opacity-50 transition-colors"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${manifestFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* API-level fetch error (network / auth / 5xx) */}
      {manifestError && !manifest && (
        <ErrorBanner message={`Could not load manifest: ${manifestError.message}`} />
      )}

      {/* Folder access error returned inside a successful API response */}
      {manifest && (!manifest.folder_online || manifest.error) && (
        <div className="flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          <Info className="h-4 w-4 shrink-0 mt-0.5" />
          <span>
            {manifest.error
              ? manifest.error
              : <>
                  <strong>OneDrive folder offline</strong> — <code className="text-xs bg-amber-100 px-1 rounded">{manifest.base_folder}</code> is not accessible.
                  Showing last-known manifest state.
                </>
            }
            {' '}Ingestion will fail until the folder is available.
          </span>
        </div>
      )}

      {/* Manifest summary card */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Manifest Summary</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Summary stats row */}
          <div className="grid grid-cols-3 gap-4 pb-4 border-b border-secondary-100">
            <div>
              <p className="text-xs text-secondary-500 uppercase tracking-wide mb-1">PDFs Tracked</p>
              <p className="text-2xl font-bold text-secondary-900 tabular-nums">
                {manifestLoading ? <span className="h-7 w-12 bg-secondary-200 rounded animate-pulse inline-block" /> : formatNumber(manifest?.total_tracked ?? 0)}
              </p>
            </div>
            <div>
              <p className="text-xs text-secondary-500 uppercase tracking-wide mb-1">New / Changed</p>
              <p className={`text-2xl font-bold tabular-nums ${(manifest?.new_files ?? 0) + (manifest?.changed_files ?? 0) > 0 ? 'text-blue-600' : 'text-secondary-900'}`}>
                {manifestLoading ? <span className="h-7 w-12 bg-secondary-200 rounded animate-pulse inline-block" /> : `${(manifest?.new_files ?? 0) + (manifest?.changed_files ?? 0)}`}
              </p>
            </div>
            <div>
              <p className="text-xs text-secondary-500 uppercase tracking-wide mb-1">Funds Configured</p>
              <p className="text-2xl font-bold text-secondary-900 tabular-nums">
                {manifestLoading ? <span className="h-7 w-12 bg-secondary-200 rounded animate-pulse inline-block" /> : formatNumber(manifest?.by_fund?.length ?? 0)}
              </p>
            </div>
          </div>

          {/* Per-fund table */}
          <div className="divide-y divide-secondary-100">
            {manifestLoading ? (
              [...Array(4)].map((_, i) => (
                <div key={i} className="py-3 flex items-center justify-between">
                  <div className="h-4 w-48 bg-secondary-200 rounded animate-pulse" />
                  <div className="h-5 w-20 bg-secondary-100 rounded animate-pulse" />
                </div>
              ))
            ) : (
              (manifest?.by_fund ?? []).map(fund => (
                <div key={fund.folder}>
                  <button
                    onClick={() => toggleFund(fund.folder)}
                    className="w-full py-3 flex items-center justify-between text-left hover:bg-secondary-50 transition-colors px-1 rounded"
                  >
                    <div className="flex items-center gap-3">
                      {expandedFunds[fund.folder]
                        ? <ChevronUp className="h-3.5 w-3.5 text-secondary-400" />
                        : <ChevronDown className="h-3.5 w-3.5 text-secondary-400" />
                      }
                      <span className="text-sm font-medium text-secondary-800">{fund.fund_name}</span>
                      <span className="text-xs text-secondary-400">{fund.file_count} file{fund.file_count !== 1 ? 's' : ''}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      {fund.last_ingested && (
                        <span className="text-xs text-secondary-400">Last: {formatRelativeDate(fund.last_ingested)}</span>
                      )}
                      <FundStatusBadge files={fund.files} />
                    </div>
                  </button>

                  {expandedFunds[fund.folder] && fund.files.length > 0 && (
                    <div className="ml-6 mb-2 overflow-hidden rounded border border-secondary-100">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="bg-secondary-50 border-b border-secondary-100">
                            <th className="text-left py-2 px-3 font-medium text-secondary-500">Filename</th>
                            <th className="text-left py-2 px-3 font-medium text-secondary-500">Ingested</th>
                            <th className="text-right py-2 px-3 font-medium text-secondary-500">Status</th>
                          </tr>
                        </thead>
                        <tbody>
                          {fund.files.map(f => (
                            <tr key={f.filename} className="border-b border-secondary-50 hover:bg-secondary-50">
                              <td className="py-2 px-3 font-mono text-secondary-700">{f.filename}</td>
                              <td className="py-2 px-3 text-secondary-500">{f.ingested_at ? formatRelativeDate(f.ingested_at) : '—'}</td>
                              <td className="py-2 px-3 text-right"><FileStatusBadge status={f.status} /></td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Run controls */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Run Controls</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {triggerError && <ErrorBanner message={triggerError} />}

          {/* Fund checklist */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-secondary-500 uppercase tracking-wide">Select Funds to Re-ingest</span>
              <div className="flex items-center gap-2">
                <button
                  onClick={selectAll}
                  disabled={isRunning || isTriggering}
                  className="text-xs text-primary-600 hover:text-primary-800 disabled:opacity-40"
                >
                  Select All
                </button>
                <span className="text-secondary-300">·</span>
                <button
                  onClick={deselectAll}
                  disabled={isRunning || isTriggering}
                  className="text-xs text-secondary-500 hover:text-secondary-700 disabled:opacity-40"
                >
                  Deselect All
                </button>
              </div>
            </div>

            <div className="rounded-md border border-secondary-200 divide-y divide-secondary-100 overflow-hidden">
              {(manifest?.by_fund ?? []).length === 0 ? (
                <p className="py-3 px-3 text-sm text-secondary-400 italic">No funds configured</p>
              ) : (
                (manifest?.by_fund ?? []).map(fund => {
                  const checked = selectedFunds.has(fund.folder)
                  const hasNew = fund.files.some(f => f.status === 'new' || f.status === 'changed')
                  return (
                    <label
                      key={fund.folder}
                      className={`flex items-center gap-3 px-3 py-2.5 cursor-pointer hover:bg-secondary-50 transition-colors ${isRunning || isTriggering ? 'opacity-50 cursor-not-allowed' : ''}`}
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => !isRunning && !isTriggering && toggleFundSelection(fund.folder)}
                        className="h-4 w-4 rounded border-secondary-300 text-primary-600 focus:ring-primary-500"
                        disabled={isRunning || isTriggering}
                      />
                      <span className="flex-1 min-w-0">
                        <span className="text-sm font-medium text-secondary-800">{fund.fund_name}</span>
                        <span className="ml-2 text-xs text-secondary-400">{fund.file_count} file{fund.file_count !== 1 ? 's' : ''}</span>
                      </span>
                      <span className="flex items-center gap-2 shrink-0">
                        {fund.last_ingested && (
                          <span className="text-xs text-secondary-400 hidden sm:inline">
                            Last: {formatRelativeDate(fund.last_ingested)}
                          </span>
                        )}
                        <FundStatusBadge files={fund.files} />
                      </span>
                    </label>
                  )
                })
              )}
            </div>

            {selectedFunds.size > 0 && (
              <p className="mt-1.5 text-xs text-secondary-400">
                {selectedFunds.size} fund{selectedFunds.size !== 1 ? 's' : ''} selected
              </p>
            )}
          </div>

          {/* Action buttons */}
          <div className="flex flex-wrap items-center gap-3 pt-1">
            {/* Run new/changed files only */}
            <button
              onClick={() => handleRun(false)}
              disabled={isRunning || isTriggering || !manifest?.folder_online}
              className="flex items-center gap-2 px-4 py-2 text-sm font-semibold text-white bg-primary-700 rounded-md hover:bg-primary-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {isRunning || isTriggering
                ? <Loader2 className="h-4 w-4 animate-spin" />
                : <Play className="h-4 w-4" />
              }
              {isRunning ? 'Running…' : selectedFunds.size > 0 ? `Run ${selectedFunds.size} Fund${selectedFunds.size !== 1 ? 's' : ''}` : 'Run All Funds'}
            </button>

            {/* Force re-ingest selected */}
            <button
              onClick={openConfirmDialog}
              disabled={isRunning || isTriggering || !manifest?.folder_online || selectedFunds.size === 0}
              className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-amber-800 border border-amber-300 rounded-md bg-amber-50 hover:bg-amber-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Re-ingest Selected
            </button>

            {!manifest?.folder_online && (
              <span className="text-xs text-amber-600">Folder offline — cannot run</span>
            )}
          </div>

          <p className="text-xs text-secondary-400">
            <strong>Run</strong> processes only new and changed files.
            <strong> Re-ingest Selected</strong> force-reprocesses all files for the chosen funds, ignoring the manifest cache.
          </p>
        </CardContent>
      </Card>

      {/* Confirmation dialog */}
      {confirmDialog && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl border border-secondary-200 w-full max-w-md mx-4 p-6 space-y-4">
            <div className="flex items-start gap-3">
              <div className="h-10 w-10 rounded-full bg-amber-100 flex items-center justify-center shrink-0">
                <RefreshCw className="h-5 w-5 text-amber-600" />
              </div>
              <div>
                <h3 className="text-base font-semibold text-secondary-900">Re-ingest {confirmDialog.funds.length} fund{confirmDialog.funds.length !== 1 ? 's' : ''}?</h3>
                <p className="text-sm text-secondary-500 mt-1">
                  This will reprocess all <strong>{confirmDialog.fileCount}</strong> file{confirmDialog.fileCount !== 1 ? 's' : ''} for the selected funds,
                  ignoring the manifest cache. Existing holdings from these funds will be deleted and re-extracted.
                </p>
              </div>
            </div>

            <div className="rounded-md bg-secondary-50 border border-secondary-200 px-3 py-2 text-sm text-secondary-600 space-y-0.5">
              {confirmDialog.funds.length === 0 ? (
                <p className="italic">All configured funds</p>
              ) : (
                confirmDialog.funds.map(f => {
                  const fund = (manifest?.by_fund ?? []).find(mf => mf.folder === f)
                  return <p key={f}>• {fund?.fund_name ?? f}</p>
                })
              )}
            </div>

            <div className="flex justify-end gap-3 pt-1">
              <button
                onClick={() => setConfirmDialog(null)}
                className="px-4 py-2 text-sm font-medium text-secondary-700 border border-secondary-200 rounded-md hover:bg-secondary-50 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => handleRun(true)}
                disabled={isTriggering}
                className="flex items-center gap-2 px-4 py-2 text-sm font-semibold text-white bg-amber-600 rounded-md hover:bg-amber-700 disabled:opacity-50 transition-colors"
              >
                {isTriggering ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                Confirm Re-ingest
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Live progress — shown while running */}
      {isRunning && (
        <Card className="border-blue-200">
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
              Live Progress
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Progress bar */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-secondary-700">
                  Files: {(progress.files_processed ?? 0) + (progress.files_skipped ?? 0)} / {progress.total_files ?? '?'}
                </span>
                <span className="text-sm font-semibold text-secondary-900 tabular-nums">{filePct}%</span>
              </div>
              <ProgressBar value={filePct} color="bg-blue-500" animated />
            </div>

            {/* Current fund */}
            {progress.current_fund && (
              <p className="text-sm text-secondary-600">
                Processing: <span className="font-medium text-secondary-900">{progress.current_fund}</span>
              </p>
            )}

            {/* Fund progress */}
            <div className="text-xs text-secondary-500">
              Fund {progress.funds_complete ?? 0} of {progress.total_funds ?? '?'} complete
            </div>

            {/* Live counters */}
            <div className="grid grid-cols-3 gap-3">
              {[
                { label: 'Files Processed', value: progress.files_processed ?? 0 },
                { label: 'Files Skipped', value: progress.files_skipped ?? 0 },
                { label: 'Holdings Extracted', value: progress.holdings_extracted ?? 0 },
              ].map(stat => (
                <div key={stat.label} className="rounded-lg bg-secondary-50 p-3 text-center">
                  <p className="text-lg font-bold text-secondary-900 tabular-nums">{stat.value.toLocaleString()}</p>
                  <p className="text-xs text-secondary-500 mt-0.5">{stat.label}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Results summary — shown after completion */}
      {hasResult && runStatus?.last_result && (
        <Card className={isFailed ? 'border-red-200' : 'border-green-200'}>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              {isFailed
                ? <XCircle className="h-4 w-4 text-red-500" />
                : <CheckCircle className="h-4 w-4 text-green-500" />
              }
              {isFailed ? 'Run Failed' : 'Run Complete'}
              {runStatus?.completed_at && (
                <span className="text-xs text-secondary-400 font-normal ml-2">
                  {formatTimestamp(runStatus.completed_at)}
                </span>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {isFailed && runStatus.error && (
              <ErrorBanner message={runStatus.error} />
            )}

            {isComplete && (
              <>
                {/* Summary counters */}
                <div className="grid grid-cols-4 gap-3">
                  {[
                    { label: 'Funds', value: runStatus.last_result.length },
                    { label: 'Files Processed', value: runStatus.last_result.reduce((s, r) => s + r.processed, 0) },
                    { label: 'Files Skipped', value: runStatus.last_result.reduce((s, r) => s + r.skipped, 0) },
                    { label: 'Holdings Extracted', value: runStatus.last_result.reduce((s, r) => s + r.holdings, 0) },
                  ].map(stat => (
                    <div key={stat.label} className="rounded-lg bg-green-50 p-3 text-center">
                      <p className="text-xl font-bold text-secondary-900 tabular-nums">{stat.value.toLocaleString()}</p>
                      <p className="text-xs text-secondary-500 mt-0.5">{stat.label}</p>
                    </div>
                  ))}
                </div>

                {/* Per-fund breakdown */}
                <div className="overflow-hidden rounded border border-secondary-200">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-secondary-50 border-b border-secondary-200">
                        <th className="text-left py-2 px-3 font-medium text-secondary-600 text-xs uppercase tracking-wide">Fund</th>
                        <th className="text-right py-2 px-3 font-medium text-secondary-600 text-xs uppercase tracking-wide">Processed</th>
                        <th className="text-right py-2 px-3 font-medium text-secondary-600 text-xs uppercase tracking-wide">Skipped</th>
                        <th className="text-right py-2 px-3 font-medium text-secondary-600 text-xs uppercase tracking-wide">Holdings</th>
                        <th className="text-right py-2 px-3 font-medium text-secondary-600 text-xs uppercase tracking-wide">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {runStatus.last_result.map((r, i) => (
                        <tr key={i} className="border-b border-secondary-100 hover:bg-secondary-50">
                          <td className="py-2 px-3 font-medium text-secondary-800">{r.fund}</td>
                          <td className="py-2 px-3 text-right tabular-nums text-secondary-700">{r.processed}</td>
                          <td className="py-2 px-3 text-right tabular-nums text-secondary-500">{r.skipped}</td>
                          <td className="py-2 px-3 text-right tabular-nums text-secondary-700">{r.holdings}</td>
                          <td className="py-2 px-3 text-right">
                            {r.errors > 0
                              ? <span className="text-xs text-amber-600">{r.errors} errors</span>
                              : <span className="text-xs text-green-600">OK</span>
                            }
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Run pipeline hint */}
                <div className="flex items-start gap-3 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800">
                  <Info className="h-4 w-4 shrink-0 mt-0.5" />
                  <div>
                    <strong>Next step:</strong> Run the full pipeline to process entity resolution, GICS mapping, and exposure aggregation.
                    <div className="mt-1.5 font-mono text-xs bg-blue-100 rounded px-2 py-1 inline-block">
                      python run_pipeline.py --pdf
                    </div>
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* Log viewer */}
      {showLogs ? (
        <div>
          <LogViewer lines={localLogs} onClear={() => setLocalLogs([])} />
          {!isRunning && localLogs.length > 0 && (
            <button
              onClick={() => setShowLogs(false)}
              className="mt-2 text-xs text-secondary-400 hover:text-secondary-600"
            >
              Hide log
            </button>
          )}
        </div>
      ) : (
        <button
          onClick={() => { setShowLogs(true); fetchLogs() }}
          className="flex items-center gap-1.5 text-xs text-secondary-500 hover:text-secondary-700 transition-colors"
        >
          <Terminal className="h-3.5 w-3.5" />
          Show ingestion log
        </button>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function PipelineMonitorPage() {
  const {
    data: dashStats,
    isLoading: dashLoading,
    error: dashError,
  } = useQuery({
    queryKey: ['dashboard-stats'],
    queryFn: () => fetchJSON('/api/dashboard/stats'),
    staleTime: 60 * 1000,
  })

  const { data: queueStats } = useQuery({
    queryKey: ['review-queue-stats'],
    queryFn: () => fetchJSON('/api/review-queue/stats'),
    staleTime: 30 * 1000,
  })

  const {
    data: pipelineStats,
    isLoading: pipelineLoading,
    error: pipelineError,
  } = useQuery({
    queryKey: ['pipeline-stats'],
    queryFn: () => fetchJSON('/api/pipeline/stats'),
    staleTime: 60 * 1000,
  })

  const isLoading = dashLoading || pipelineLoading
  const error = dashError || pipelineError

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Pipeline Monitor</h1>
        <p className="text-secondary-500 mt-1">Data quality, coverage, pipeline history, and document ingestion</p>
      </div>

      {error && <ErrorBanner message={`Failed to load stats: ${(dashError || pipelineError).message}`} />}

      {/* Summary stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        {isLoading ? (
          [...Array(5)].map((_, i) => <StatSkeleton key={i} />)
        ) : (
          <>
            <StatCard
              title="Total Holdings"
              value={formatNumber(dashStats?.total_holdings)}
              description={`${formatNumber(dashStats?.data_sources)} data sources`}
              icon={Layers}
            />
            <StatCard
              title="Total Companies"
              value={formatNumber(pipelineStats?.total_companies)}
              description="Canonical entities"
              icon={Database}
            />
            <StatCard
              title="Classified"
              value={formatNumber(pipelineStats?.classified_companies)}
              description={`${pipelineStats?.classification_coverage ?? 0}% coverage`}
              icon={CheckCircle2}
              colorClass="text-green-700"
            />
            <StatCard
              title="Unclassified"
              value={formatNumber(pipelineStats?.unclassified_companies)}
              description="Need AI classification"
              colorClass={pipelineStats?.unclassified_companies > 0 ? 'text-yellow-600' : 'text-secondary-900'}
            />
            <StatCard
              title="Review Queue"
              value={formatNumber(queueStats?.pending)}
              description={`${formatNumber(queueStats?.high_priority)} high priority`}
              icon={ClipboardList}
              colorClass={queueStats?.high_priority > 0 ? 'text-red-600' : 'text-secondary-900'}
            />
          </>
        )}
      </div>

      {/* Data quality + source breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-primary-600" />
              Data Quality Coverage
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            {pipelineLoading ? (
              [...Array(3)].map((_, i) => (
                <div key={i} className="space-y-1.5">
                  <div className="flex justify-between">
                    <div className="h-3 w-40 bg-secondary-200 rounded animate-pulse" />
                    <div className="h-3 w-12 bg-secondary-200 rounded animate-pulse" />
                  </div>
                  <div className="h-2.5 w-full bg-secondary-100 rounded-full animate-pulse" />
                </div>
              ))
            ) : (
              <>
                <CoverageRow
                  label="Classification Coverage"
                  value={pipelineStats?.classification_coverage}
                  description={`${formatNumber(pipelineStats?.classified_companies)} of ${formatNumber(pipelineStats?.total_companies)} companies have a primary sector`}
                />
                <CoverageRow
                  label="Entity Resolution Rate"
                  value={pipelineStats?.entity_resolution_rate}
                  description="Holdings successfully matched to a canonical company ID"
                />
                <CoverageRow
                  label="Holdings with Reported Value"
                  value={pipelineStats?.holdings_with_value_pct}
                  description="Holdings with a non-null reported_value_usd"
                />
              </>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Holdings by Source</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {pipelineLoading ? (
              <div className="space-y-3">
                <div className="h-7 w-full bg-secondary-200 rounded animate-pulse" />
                <div className="h-3 w-48 bg-secondary-100 rounded animate-pulse" />
              </div>
            ) : (
              <SourceBreakdownBar holdingsBySource={pipelineStats?.holdings_by_source ?? {}} />
            )}
            {!pipelineLoading && pipelineStats?.holdings_by_source && (
              <div className="pt-2 border-t border-secondary-100 grid grid-cols-2 gap-3 text-sm">
                {Object.entries(pipelineStats.holdings_by_source)
                  .sort((a, b) => b[1] - a[1])
                  .map(([source, count]) => {
                    const cfg = SOURCE_CONFIG[source] ?? { label: source }
                    return (
                      <div key={source}>
                        <p className="text-secondary-500 text-xs">{cfg.label}</p>
                        <p className="font-bold text-secondary-900 mt-0.5">{formatNumber(count)}</p>
                      </div>
                    )
                  })
                }
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Recent pipeline runs */}
      <Card>
        <CardHeader>
          <CardTitle>Recent Pipeline Runs</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-secondary-200 bg-secondary-50">
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Run ID</th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Timestamp</th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Details</th>
              </tr>
            </thead>
            <tbody>
              {pipelineLoading ? (
                [...Array(3)].map((_, i) => (
                  <tr key={i} className="border-b border-secondary-100">
                    <td className="py-3 px-4"><div className="h-3 w-20 bg-secondary-200 rounded animate-pulse" /></td>
                    <td className="py-3 px-4"><div className="h-3 w-36 bg-secondary-100 rounded animate-pulse" /></td>
                    <td className="py-3 px-4"><div className="h-3 w-64 bg-secondary-100 rounded animate-pulse" /></td>
                  </tr>
                ))
              ) : pipelineStats?.recent_runs?.length ? (
                pipelineStats.recent_runs.map((run) => (
                  <tr key={run.audit_event_id} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
                    <td className="py-3 px-4 font-mono text-xs text-secondary-600">
                      {run.run_id ? run.run_id.slice(0, 8) + '…' : '—'}
                    </td>
                    <td className="py-3 px-4 text-xs text-secondary-600 whitespace-nowrap">
                      {formatTimestamp(run.event_time)}
                    </td>
                    <td className="py-3 px-4 text-xs text-secondary-500 max-w-xs">
                      <span className="truncate block" title={run.payload_json}>
                        {run.payload_json ? run.payload_json.slice(0, 100) + (run.payload_json.length > 100 ? '…' : '') : 'No payload'}
                      </span>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={3} className="py-12 text-center">
                    <Activity className="h-8 w-8 text-secondary-200 mx-auto mb-2" />
                    <p className="text-secondary-400 text-sm">No pipeline runs recorded yet.</p>
                    <p className="text-secondary-300 text-xs mt-1">
                      Run <code className="bg-secondary-100 px-1 rounded">python run_pipeline.py</code> to populate this.
                    </p>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Divider */}
      <div className="border-t border-secondary-200 pt-2" />

      {/* Document ingestion section */}
      <DocumentIngestionSection />
    </div>
  )
}

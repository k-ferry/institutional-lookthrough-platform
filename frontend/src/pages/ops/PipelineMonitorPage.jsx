import { useQuery } from '@tanstack/react-query'
import {
  AlertCircle,
  Database,
  CheckCircle2,
  Activity,
  ClipboardList,
  Layers,
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

function formatNumber(n) {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
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
    })
  } catch {
    return ts
  }
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

function ProgressBar({ value, color = 'bg-primary-600' }) {
  const pct = Math.min(100, Math.max(0, value ?? 0))
  return (
    <div className="w-full bg-secondary-100 rounded-full h-2.5 overflow-hidden">
      <div
        className={`${color} h-2.5 rounded-full transition-all duration-500`}
        style={{ width: `${pct}%` }}
      />
    </div>
  )
}

function CoverageRow({ label, value, description, color }) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-secondary-700">{label}</span>
        <span className={`text-sm font-bold tabular-nums ${value >= 80 ? 'text-green-600' : value >= 50 ? 'text-yellow-600' : 'text-red-600'}`}>
          {value != null ? `${value.toFixed(1)}%` : '—'}
        </span>
      </div>
      <ProgressBar
        value={value}
        color={value >= 80 ? 'bg-green-500' : value >= 50 ? 'bg-yellow-500' : 'bg-red-500'}
      />
      {description && <p className="text-xs text-secondary-400">{description}</p>}
    </div>
  )
}

function SourceBreakdownBar({ bdc, synthetic, total }) {
  if (!total) return <p className="text-sm text-secondary-400">No holdings data</p>
  const bdcPct = (bdc / total) * 100
  const synPct = (synthetic / total) * 100
  return (
    <div className="space-y-3">
      <div className="flex h-7 rounded-md overflow-hidden text-xs font-medium">
        {bdcPct > 0 && (
          <div
            className="bg-primary-600 flex items-center justify-center text-white"
            style={{ width: `${bdcPct}%` }}
            title={`BDC: ${formatNumber(bdc)}`}
          >
            {bdcPct > 8 ? `${bdcPct.toFixed(0)}%` : ''}
          </div>
        )}
        {synPct > 0 && (
          <div
            className="bg-secondary-300 flex items-center justify-center text-secondary-700"
            style={{ width: `${synPct}%` }}
            title={`Synthetic: ${formatNumber(synthetic)}`}
          >
            {synPct > 8 ? `${synPct.toFixed(0)}%` : ''}
          </div>
        )}
      </div>
      <div className="flex items-center gap-4 text-xs text-secondary-600">
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-2.5 rounded-sm bg-primary-600 inline-block" />
          BDC / Real ({formatNumber(bdc)} holdings)
        </span>
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-2.5 rounded-sm bg-secondary-300 inline-block" />
          Synthetic ({formatNumber(synthetic)} holdings)
        </span>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
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
        <p className="text-secondary-500 mt-1">Data quality, coverage, and pipeline run history</p>
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

      {/* Data quality + source breakdown row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Data quality coverage */}
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

        {/* Source breakdown */}
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
              <SourceBreakdownBar
                bdc={pipelineStats?.bdc_holdings ?? 0}
                synthetic={pipelineStats?.synthetic_holdings ?? 0}
                total={pipelineStats?.total_holdings ?? 0}
              />
            )}

            {!pipelineLoading && pipelineStats && (
              <div className="pt-2 border-t border-secondary-100 grid grid-cols-2 gap-4 text-sm">
                <div>
                  <p className="text-secondary-500 text-xs">BDC Holdings</p>
                  <p className="font-bold text-secondary-900 mt-0.5">
                    {formatNumber(pipelineStats.bdc_holdings)}
                  </p>
                  <p className="text-xs text-secondary-400">ARCC · MAIN · OBDC</p>
                </div>
                <div>
                  <p className="text-secondary-500 text-xs">Synthetic Holdings</p>
                  <p className="font-bold text-secondary-900 mt-0.5">
                    {formatNumber(pipelineStats.synthetic_holdings)}
                  </p>
                  <p className="text-xs text-secondary-400">Generated test data</p>
                </div>
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
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Run ID
                </th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Timestamp
                </th>
                <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">
                  Details
                </th>
              </tr>
            </thead>
            <tbody>
              {pipelineLoading ? (
                [...Array(3)].map((_, i) => (
                  <tr key={i} className="border-b border-secondary-100">
                    <td className="py-3 px-4">
                      <div className="h-3 w-20 bg-secondary-200 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-4">
                      <div className="h-3 w-36 bg-secondary-100 rounded animate-pulse" />
                    </td>
                    <td className="py-3 px-4">
                      <div className="h-3 w-64 bg-secondary-100 rounded animate-pulse" />
                    </td>
                  </tr>
                ))
              ) : pipelineStats?.recent_runs?.length ? (
                pipelineStats.recent_runs.map((run) => (
                  <tr
                    key={run.audit_event_id}
                    className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors"
                  >
                    <td className="py-3 px-4 font-mono text-xs text-secondary-600">
                      {run.run_id ? run.run_id.slice(0, 8) + '…' : '—'}
                    </td>
                    <td className="py-3 px-4 text-xs text-secondary-600 whitespace-nowrap">
                      {formatTimestamp(run.event_time)}
                    </td>
                    <td className="py-3 px-4 text-xs text-secondary-500 max-w-xs">
                      <span className="truncate block" title={run.payload_json}>
                        {run.payload_json
                          ? run.payload_json.slice(0, 100) + (run.payload_json.length > 100 ? '…' : '')
                          : 'No payload'}
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
    </div>
  )
}

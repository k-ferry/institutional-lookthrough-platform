import { useParams, Link, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  ArrowLeft,
  Building2,
  AlertCircle,
} from 'lucide-react'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: 'include' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function formatCurrency(value) {
  if (value === null || value === undefined) return '—'
  if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`
  return `$${value.toLocaleString()}`
}

function formatDate(str) {
  if (!str) return '—'
  return str.slice(0, 10)
}

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

function confidenceClasses(conf) {
  if (conf === null || conf === undefined) return 'bg-gray-100 text-gray-500'
  if (conf >= 0.7) return 'bg-green-100 text-green-800'
  if (conf >= 0.5) return 'bg-yellow-100 text-yellow-800'
  return 'bg-red-100 text-red-700'
}

function confidenceLabel(conf) {
  if (conf === null || conf === undefined) return '—'
  return `${(conf * 100).toFixed(0)}%`
}

const ACTION_COLORS = {
  entity_resolution: 'bg-blue-100 text-blue-700',
  ai_classification: 'bg-purple-100 text-purple-700',
  review_queue_insert: 'bg-amber-100 text-amber-700',
  pipeline_run_complete: 'bg-green-100 text-green-700',
}

function actionChipClasses(action) {
  return ACTION_COLORS[action] ?? 'bg-gray-100 text-gray-600'
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

function SectionHeader({ children }) {
  return (
    <h2 className="text-lg font-semibold text-secondary-900 mb-3">{children}</h2>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function CompanyDetailPage() {
  const { company_id } = useParams()
  const navigate = useNavigate()

  const { data: company, isLoading, error } = useQuery({
    queryKey: ['company-detail', company_id],
    queryFn: () => fetchJSON(`/api/companies/${company_id}`),
    staleTime: 5 * 60 * 1000,
    retry: 1,
  })

  // ---- Loading skeleton ----
  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-5 w-36 bg-secondary-200 rounded animate-pulse" />
        <div className="h-8 w-72 bg-secondary-200 rounded animate-pulse" />
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="h-48 bg-secondary-100 rounded-lg animate-pulse" />
          <div className="h-48 bg-secondary-100 rounded-lg animate-pulse" />
        </div>
      </div>
    )
  }

  // ---- Error / not found ----
  if (error || !company) {
    return (
      <div className="space-y-4">
        <button
          onClick={() => navigate(-1)}
          className="inline-flex items-center gap-1.5 text-sm text-secondary-500 hover:text-secondary-700 transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          Back
        </button>
        <ErrorBanner
          message={error ? `Failed to load company: ${error.message}` : 'Company not found.'}
        />
      </div>
    )
  }

  const cls = company.classification
  const res = company.resolution

  return (
    <div className="space-y-6">

      {/* Back navigation */}
      <button
        onClick={() => navigate(-1)}
        className="inline-flex items-center gap-1.5 text-sm text-secondary-500 hover:text-secondary-700 transition-colors"
      >
        <ArrowLeft className="h-4 w-4" />
        Back
      </button>

      {/* Header */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 rounded-full bg-primary-50 flex items-center justify-center shrink-0">
            <Building2 className="h-5 w-5 text-primary-600" />
          </div>
          <h1 className="text-2xl font-bold text-secondary-900">{company.company_name}</h1>
        </div>
        <div className="flex flex-wrap items-center gap-2 ml-13 pl-1">
          {company.primary_sector && (
            <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${sectorClasses(company.primary_sector)}`}>
              {company.primary_sector}
            </span>
          )}
          {company.primary_industry && company.primary_industry !== company.primary_sector && (
            <span className="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium bg-secondary-100 text-secondary-600">
              {company.primary_industry}
            </span>
          )}
          {company.source && (
            <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-mono font-medium ${
              company.source === 'synthetic'
                ? 'bg-violet-100 text-violet-700'
                : 'bg-amber-100 text-amber-700'
            }`}>
              {company.source}
            </span>
          )}
          {company.primary_country && (
            <span className="text-xs text-secondary-400">{company.primary_country}</span>
          )}
        </div>
      </div>

      {/* Two-column: Classification + Resolution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* GICS Classification */}
        <Card>
          <CardHeader>
            <CardTitle>GICS Classification</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-secondary-500">Sector</span>
                <span className="font-medium text-secondary-800">{company.primary_sector ?? '—'}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-secondary-500">Industry</span>
                <span className="font-medium text-secondary-800">{company.primary_industry ?? '—'}</span>
              </div>
            </div>

            <div className="border-t border-secondary-100 pt-3">
              {cls ? (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-secondary-500 uppercase tracking-wide font-medium">AI Classification</span>
                    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${confidenceClasses(cls.confidence)}`}>
                      {confidenceLabel(cls.confidence)} confidence
                    </span>
                  </div>
                  {cls.rationale && (
                    <p className="text-xs text-secondary-600 leading-relaxed">{cls.rationale}</p>
                  )}
                  {cls.model && (
                    <p className="text-xs text-secondary-400 font-mono">{cls.model}</p>
                  )}
                </div>
              ) : company.primary_sector ? (
                <p className="text-xs text-secondary-400">Classified via reported sector</p>
              ) : (
                <p className="text-xs text-secondary-400">Unclassified</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Entity Resolution */}
        <Card>
          <CardHeader>
            <CardTitle>Entity Resolution</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-secondary-500">Method(s)</span>
                <span className="font-medium text-secondary-800">
                  {res.match_methods.length ? res.match_methods.join(', ') : '—'}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-secondary-500">Avg. Confidence</span>
                <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${confidenceClasses(res.avg_confidence)}`}>
                  {confidenceLabel(res.avg_confidence)}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-secondary-500">Resolved Holdings</span>
                <span className="font-medium text-secondary-800">{res.resolution_count.toLocaleString()}</span>
              </div>
            </div>

            {res.raw_names.length > 0 && (
              <div className="border-t border-secondary-100 pt-3 space-y-1">
                <p className="text-xs text-secondary-500 uppercase tracking-wide font-medium mb-2">
                  Known Aliases / Raw Names
                </p>
                <div className="space-y-1 max-h-36 overflow-y-auto">
                  {res.raw_names.map((name, i) => (
                    <p key={i} className="text-xs text-secondary-700 font-mono bg-secondary-50 rounded px-2 py-0.5 truncate" title={name}>
                      {name}
                    </p>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Fund Exposure */}
      <div>
        <SectionHeader>
          Fund Exposure
          {company.fund_exposure.length > 0 && (
            <span className="ml-2 text-sm font-normal text-secondary-400">
              Held across {company.fund_exposure.length} fund{company.fund_exposure.length !== 1 ? 's' : ''}
            </span>
          )}
        </SectionHeader>
        <Card className="overflow-hidden">
          {!company.fund_exposure.length ? (
            <div className="py-10 text-center text-secondary-400 text-sm">No fund exposure data available.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-secondary-200 bg-secondary-50">
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Fund</th>
                    <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Holdings</th>
                    <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Total Value</th>
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Most Recent</th>
                  </tr>
                </thead>
                <tbody>
                  {company.fund_exposure.map((f) => (
                    <tr key={f.fund_id} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
                      <td className="py-3 px-4">
                        <Link
                          to={`/funds/${f.fund_id}`}
                          className="font-medium text-primary-700 hover:text-primary-900 hover:underline"
                        >
                          {f.fund_name}
                        </Link>
                      </td>
                      <td className="py-3 px-4 text-right tabular-nums text-secondary-700">
                        {f.holding_count.toLocaleString()}
                      </td>
                      <td className="py-3 px-4 text-right tabular-nums font-medium text-secondary-800">
                        {f.total_value != null ? formatCurrency(f.total_value) : '—'}
                      </td>
                      <td className="py-3 px-4 text-secondary-500 text-xs">
                        {formatDate(f.most_recent_date)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>

      {/* Holdings History */}
      <div>
        <SectionHeader>Holdings History</SectionHeader>
        <Card className="overflow-hidden">
          {!company.holdings.length ? (
            <div className="py-10 text-center text-secondary-400 text-sm">No holdings records found.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-secondary-200 bg-secondary-50">
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Fund</th>
                    <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Value</th>
                    <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">% NAV</th>
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Reported Sector</th>
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">As of Date</th>
                    <th className="text-left py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Source</th>
                    <th className="text-right py-3 px-4 font-semibold text-secondary-600 text-xs uppercase tracking-wide">Extraction</th>
                  </tr>
                </thead>
                <tbody>
                  {company.holdings.map((h) => (
                    <tr key={h.reported_holding_id} className="border-b border-secondary-100 hover:bg-secondary-50 transition-colors">
                      <td className="py-2.5 px-4">
                        <Link
                          to={`/funds/${h.fund_id}`}
                          className="text-secondary-600 hover:text-primary-700 hover:underline text-xs"
                        >
                          {h.fund_name}
                        </Link>
                      </td>
                      <td className="py-2.5 px-4 text-right tabular-nums font-medium text-secondary-800 text-xs">
                        {h.reported_value_usd != null ? formatCurrency(h.reported_value_usd) : '—'}
                      </td>
                      <td className="py-2.5 px-4 text-right tabular-nums text-secondary-500 text-xs">
                        {h.reported_pct_nav != null ? `${h.reported_pct_nav.toFixed(2)}%` : '—'}
                      </td>
                      <td className="py-2.5 px-4 text-secondary-600 text-xs max-w-[180px] truncate" title={h.reported_sector ?? ''}>
                        {h.reported_sector ?? '—'}
                      </td>
                      <td className="py-2.5 px-4 text-secondary-500 text-xs">{formatDate(h.as_of_date)}</td>
                      <td className="py-2.5 px-4 text-xs text-secondary-400">{h.source ?? '—'}</td>
                      <td className="py-2.5 px-4 text-right">
                        {h.extraction_confidence != null ? (
                          <span className={`inline-flex items-center rounded-full px-1.5 py-0.5 text-xs font-medium ${confidenceClasses(h.extraction_confidence)}`}>
                            {confidenceLabel(h.extraction_confidence)}
                          </span>
                        ) : (
                          <span className="text-secondary-300 text-xs">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>

      {/* Recent Audit Events */}
      {company.audit_events.length > 0 && (
        <div>
          <SectionHeader>Recent Audit Events</SectionHeader>
          <Card className="overflow-hidden">
            <div className="divide-y divide-secondary-100">
              {company.audit_events.map((evt) => (
                <div key={evt.audit_event_id} className="flex items-start gap-3 px-4 py-3">
                  <span className={`shrink-0 inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${actionChipClasses(evt.action)}`}>
                    {evt.action.replace(/_/g, ' ')}
                  </span>
                  <div className="min-w-0 flex-1">
                    <p className="text-xs text-secondary-600">
                      <span className="font-medium text-secondary-800">{evt.actor_id}</span>
                      {evt.payload_json && (
                        <span className="ml-1 text-secondary-400 font-mono truncate">
                          · {evt.payload_json.slice(0, 80)}{evt.payload_json.length > 80 ? '…' : ''}
                        </span>
                      )}
                    </p>
                  </div>
                  <span className="shrink-0 text-xs text-secondary-400 whitespace-nowrap">
                    {formatDate(evt.event_time)}
                  </span>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}
    </div>
  )
}

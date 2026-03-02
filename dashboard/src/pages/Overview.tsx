import { useNavigate } from 'react-router-dom'
import { Briefcase, Inbox, FileText, Mail, ArrowRight, AlertCircle } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { StatusBadge, STATUS_LABELS } from '../components/StatusBadge'
import { formatTime, fieldLabel } from '../lib/format'
import type { DashboardStats, CaseListItem } from '../types/api'

const STATUS_COLORS: Record<string, string> = {
  INTAKE: 'bg-gray-400',
  WAITING_FOR_DOCUMENTS: 'bg-yellow-400',
  NEEDS_QUESTIONS_PARTNER: 'bg-orange-400',
  NEEDS_QUESTIONS_BROKER: 'bg-orange-500',
  NEEDS_MANUAL_REVIEW_BROKER: 'bg-red-400',
  AWAITING_BROKER_CONFIRMATION: 'bg-blue-400',
  READY_FOR_IMPORT: 'bg-green-400',
  IMPORTED: 'bg-emerald-500',
}

const ACTION_STATUSES = new Set([
  'NEEDS_QUESTIONS_PARTNER',
  'NEEDS_QUESTIONS_BROKER',
  'NEEDS_MANUAL_REVIEW_BROKER',
  'READY_FOR_IMPORT',
])

export function Overview() {
  const navigate = useNavigate()
  const { data: stats, loading: statsLoading } = useApiGet<DashboardStats>('/api/dashboard/stats')
  const { data: casesData, loading: casesLoading } = useApiGet<{ cases: CaseListItem[] }>('/api/dashboard/cases')

  if (statsLoading || !stats) return <LoadingSpinner />

  const cases = casesData?.cases || []
  const actionCases = cases.filter(c => ACTION_STATUSES.has(c.status))
  const recentCases = cases.slice(0, 5)

  const cards = [
    {
      label: 'Cases',
      value: stats.cases_total,
      icon: Briefcase,
      color: 'text-blue-600 bg-blue-50',
      link: '/app/cases',
    },
    {
      label: 'Triage',
      value: stats.triage_count,
      icon: Inbox,
      color: 'text-orange-600 bg-orange-50',
      link: '/app/triage',
    },
    {
      label: 'Dokumente',
      value: stats.documents_total,
      icon: FileText,
      color: 'text-emerald-600 bg-emerald-50',
      link: '/app/cases',
    },
    {
      label: 'E-Mails',
      value: stats.emails_total,
      icon: Mail,
      color: 'text-purple-600 bg-purple-50',
      link: '/app/emails',
    },
  ]

  const totalCases = Object.values(stats.cases_by_status).reduce((s, v) => s + v, 0) || 1

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Übersicht</h1>

      {/* Stat Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {cards.map(card => (
          <button
            key={card.label}
            onClick={() => navigate(card.link)}
            className="bg-white rounded-xl border border-gray-200 p-5 text-left hover:shadow-md transition-shadow"
          >
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm font-medium text-gray-500">{card.label}</span>
              <div className={`w-9 h-9 rounded-lg flex items-center justify-center ${card.color}`}>
                <card.icon size={18} />
              </div>
            </div>
            <div className="text-3xl font-bold text-gray-900">{card.value}</div>
          </button>
        ))}
      </div>

      <div className="grid lg:grid-cols-2 gap-6 mb-6">
        {/* Action Required */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              <AlertCircle size={18} className="text-orange-500" />
              Handlungsbedarf
            </h2>
            {actionCases.length > 0 && (
              <span className="text-xs font-medium bg-orange-100 text-orange-700 px-2 py-0.5 rounded-full">
                {actionCases.length}
              </span>
            )}
          </div>
          {casesLoading ? (
            <div className="text-sm text-gray-400">Laden...</div>
          ) : actionCases.length === 0 ? (
            <p className="text-sm text-gray-500">Keine offenen Aktionen</p>
          ) : (
            <div className="space-y-2">
              {actionCases.slice(0, 5).map(c => (
                <button
                  key={c.case_id}
                  onClick={() => navigate(`/app/cases/${c.case_id}`)}
                  className="w-full flex items-center justify-between gap-3 px-3 py-2.5 rounded-lg hover:bg-gray-50 transition-colors text-left"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="text-sm font-medium text-gray-900 truncate">{c.applicant_name || 'Unbekannt'}</span>
                      <StatusBadge status={c.status} />
                    </div>
                    <div className="text-xs text-gray-500">
                      {c.missing_financing.length > 0 && (
                        <span>Fehlend: {c.missing_financing.slice(0, 3).map(f => fieldLabel(f)).join(', ')}</span>
                      )}
                      {c.missing_financing.length === 0 && c.missing_docs_count > 0 && (
                        <span>{c.missing_docs_count} Dokumente fehlen</span>
                      )}
                    </div>
                  </div>
                  <ArrowRight size={14} className="text-gray-400 shrink-0" />
                </button>
              ))}
            </div>
          )}

          {/* Triage hint */}
          {stats.triage_count > 0 && (
            <button
              onClick={() => navigate('/app/triage')}
              className="w-full mt-3 flex items-center justify-between gap-3 px-3 py-2.5 rounded-lg bg-orange-50 hover:bg-orange-100 transition-colors text-left"
            >
              <div className="flex items-center gap-2">
                <Inbox size={14} className="text-orange-600" />
                <span className="text-sm text-orange-700">{stats.triage_count} E-Mails in der Triage</span>
              </div>
              <ArrowRight size={14} className="text-orange-400" />
            </button>
          )}
        </div>

        {/* Status Distribution */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Status-Verteilung</h2>

          {/* Bar */}
          <div className="flex h-4 rounded-full overflow-hidden mb-4">
            {Object.entries(stats.cases_by_status).map(([status, count]) => (
              <div
                key={status}
                className={`${STATUS_COLORS[status] || 'bg-gray-300'} transition-all`}
                style={{ width: `${(count / totalCases) * 100}%` }}
                title={`${STATUS_LABELS[status] || status}: ${count}`}
              />
            ))}
          </div>

          {/* Legend */}
          <div className="grid grid-cols-2 gap-3">
            {Object.entries(stats.cases_by_status).map(([status, count]) => (
              <div key={status} className="flex items-center gap-2">
                <div className={`w-3 h-3 rounded-full ${STATUS_COLORS[status] || 'bg-gray-300'}`} />
                <span className="text-sm text-gray-600">
                  {STATUS_LABELS[status] || status}{' '}
                  <span className="font-medium text-gray-900">({count})</span>
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Recent Cases */}
      {!casesLoading && recentCases.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900">Letzte Cases</h2>
            <button
              onClick={() => navigate('/app/cases')}
              className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
            >
              Alle anzeigen
              <ArrowRight size={14} />
            </button>
          </div>
          <div className="space-y-2">
            {recentCases.map(c => (
              <button
                key={c.case_id}
                onClick={() => navigate(`/app/cases/${c.case_id}`)}
                className="w-full flex items-center justify-between gap-3 px-3 py-2.5 rounded-lg hover:bg-gray-50 transition-colors text-left"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-sm font-medium text-gray-900 truncate">{c.applicant_name || 'Unbekannt'}</span>
                  <StatusBadge status={c.status} />
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  <span className="inline-flex items-center gap-1.5">
                    <span className="w-12 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                      <span
                        className={`block h-full rounded-full ${
                          c.completeness_pct >= 100 ? 'bg-green-500' :
                          c.completeness_pct >= 60 ? 'bg-yellow-500' : 'bg-orange-500'
                        }`}
                        style={{ width: `${c.completeness_pct}%` }}
                      />
                    </span>
                    <span className="text-xs text-gray-400 w-8">{c.completeness_pct}%</span>
                  </span>
                  <span className="text-xs text-gray-400">{formatTime(c.last_status_change)}</span>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

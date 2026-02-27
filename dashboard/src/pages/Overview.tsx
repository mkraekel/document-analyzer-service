import { useNavigate } from 'react-router-dom'
import { Briefcase, Inbox, FileText, Mail } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { STATUS_LABELS } from '../components/StatusBadge'
import type { DashboardStats } from '../types/api'

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

export function Overview() {
  const navigate = useNavigate()
  const { data: stats, loading } = useApiGet<DashboardStats>('/api/dashboard/stats')

  if (loading || !stats) return <LoadingSpinner />

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
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Uebersicht</h1>

      {/* Stat Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
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
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
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
  )
}

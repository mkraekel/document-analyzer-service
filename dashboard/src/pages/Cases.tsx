import { useState, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { StatusBadge } from '../components/StatusBadge'
import { Pagination } from '../components/Pagination'
import { formatTime, fieldLabel } from '../lib/format'
import type { CaseListItem } from '../types/api'

const PER_PAGE = 20

export function Cases() {
  const navigate = useNavigate()
  const { data, loading } = useApiGet<{ cases: CaseListItem[] }>('/api/dashboard/cases')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [showArchived, setShowArchived] = useState(false)

  const HIDDEN_STATUSES = new Set(['ARCHIVED', 'DECLINED', 'IMPORTED'])
  const cases = data?.cases || []

  const filtered = useMemo(() => {
    let result = cases
    if (!showArchived) {
      result = result.filter(c => !HIDDEN_STATUSES.has(c.status))
    }
    if (search.trim()) {
      const q = search.toLowerCase()
      result = result.filter(
        c =>
          (c.applicant_name || '').toLowerCase().includes(q) ||
          (c.partner_email || '').toLowerCase().includes(q) ||
          (c.case_id || '').toLowerCase().includes(q) ||
          (c.status || '').toLowerCase().includes(q),
      )
    }
    return result
  }, [cases, search, showArchived])

  const totalPages = Math.ceil(filtered.length / PER_PAGE)
  const pageItems = filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE)

  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Cases</h1>
          <p className="text-sm text-gray-500 mt-1">{filtered.length} Finanzierungsanfragen</p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => { setShowArchived(!showArchived); setPage(1) }}
            className={`px-3 py-2 text-sm rounded-lg border transition-colors ${
              showArchived
                ? 'bg-gray-200 text-gray-700 border-gray-300'
                : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'
            }`}
          >
            {showArchived ? 'Alle' : 'Archiv zeigen'}
          </button>
          <div className="relative">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={e => { setSearch(e.target.value); setPage(1) }}
              placeholder="Suchen..."
              className="pl-9 pr-4 py-2 border border-gray-300 rounded-lg text-sm w-full sm:w-64 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
            />
          </div>
        </div>
      </div>

      {pageItems.length === 0 ? (
        <div className="bg-white rounded-xl border border-gray-200 p-12 text-center text-gray-500">
          Keine Cases gefunden
        </div>
      ) : (
        <div className="space-y-3">
          {pageItems.map(c => (
            <button
              key={c.case_id}
              onClick={() => navigate(`/app/cases/${c.case_id}`)}
              className="w-full bg-white rounded-xl border border-gray-200 p-5 text-left hover:shadow-md transition-shadow"
            >
              <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 sm:gap-4">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-3 mb-1">
                    <span className="font-semibold text-gray-900 truncate">
                      {c.applicant_name || 'Unbekannt'}
                    </span>
                    <StatusBadge status={c.status} />
                  </div>
                  <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-gray-500">
                    {c.partner_email && <span className="truncate">{c.partner_email}</span>}
                    <span title={c.last_status_change}>{formatTime(c.last_status_change)}</span>
                    <span className="inline-flex items-center gap-1.5">
                      <span className="w-16 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                        <span
                          className={`block h-full rounded-full ${
                            c.completeness_pct >= 100 ? 'bg-green-500' :
                            c.completeness_pct >= 60 ? 'bg-yellow-500' : 'bg-orange-500'
                          }`}
                          style={{ width: `${c.completeness_pct}%` }}
                        />
                      </span>
                      <span className="text-xs text-gray-400">{c.completeness_pct}%</span>
                    </span>
                  </div>
                </div>

                {/* Completeness indicator */}
                {!c.is_complete && (
                  <div className="sm:text-right shrink-0">
                    {c.missing_financing.length > 0 && (
                      <div className="text-xs text-orange-600 mb-1">
                        Fehlende Daten: {c.missing_financing.map(f => fieldLabel(f)).join(', ')}
                      </div>
                    )}
                    {c.missing_docs_count > 0 && (
                      <div className="text-xs text-red-600">
                        {c.missing_docs_count} Dokumente fehlen
                      </div>
                    )}
                  </div>
                )}
                {c.is_complete && (
                  <div className="text-xs text-green-600 font-medium">Vollständig</div>
                )}
              </div>
            </button>
          ))}
        </div>
      )}

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  )
}

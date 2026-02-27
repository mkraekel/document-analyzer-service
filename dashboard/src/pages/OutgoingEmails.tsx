import { useState } from 'react'
import { ChevronDown, ChevronUp } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { Pagination } from '../components/Pagination'
import { formatTime } from '../lib/format'
import type { OutgoingEmail } from '../types/api'

const PER_PAGE = 20

export function OutgoingEmails() {
  const { data, loading } = useApiGet<{ emails: OutgoingEmail[] }>('/api/dashboard/outgoing-emails')
  const [page, setPage] = useState(1)
  const [expanded, setExpanded] = useState<Set<number>>(new Set())

  const emails = data?.emails || []
  const totalPages = Math.ceil(emails.length / PER_PAGE)
  const pageItems = emails.slice((page - 1) * PER_PAGE, page * PER_PAGE)

  function toggleExpand(idx: number) {
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(idx) ? next.delete(idx) : next.add(idx)
      return next
    })
  }

  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Ausgehende E-Mails</h1>
        <p className="text-sm text-gray-500 mt-1">{emails.length} E-Mails protokolliert</p>
      </div>

      {pageItems.length === 0 ? (
        <div className="bg-white rounded-xl border border-gray-200 p-12 text-center text-gray-500">
          Noch keine ausgehenden E-Mails
        </div>
      ) : (
        <div className="space-y-3">
          {pageItems.map((email, i) => {
            const globalIdx = (page - 1) * PER_PAGE + i
            const isExpanded = expanded.has(globalIdx)

            return (
              <div
                key={globalIdx}
                className="bg-white rounded-xl border border-gray-200 overflow-hidden"
              >
                <button
                  onClick={() => toggleExpand(globalIdx)}
                  className="w-full px-5 py-4 flex items-start gap-4 text-left hover:bg-gray-50 transition-colors"
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      {email.dry_run && (
                        <span className="bg-yellow-100 text-yellow-700 text-xs px-2 py-0.5 rounded-full font-medium">
                          DRY-RUN
                        </span>
                      )}
                      <span className="font-medium text-gray-900 truncate text-sm">
                        {email.subject}
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-xs text-gray-500">
                      <span>An: {email.to}</span>
                      <span title={email.logged_at}>{formatTime(email.logged_at)}</span>
                    </div>
                  </div>
                  {isExpanded ? (
                    <ChevronUp size={16} className="text-gray-400 mt-1" />
                  ) : (
                    <ChevronDown size={16} className="text-gray-400 mt-1" />
                  )}
                </button>

                {isExpanded && (
                  <div className="px-5 pb-5 border-t border-gray-100">
                    <div className="mt-3 text-sm text-gray-600 bg-gray-50 rounded-lg p-4 whitespace-pre-wrap max-h-96 overflow-y-auto">
                      {email.body_text || '(Nur HTML-Inhalt)'}
                    </div>
                    {email.body_html && (
                      <details className="mt-3">
                        <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-700">
                          HTML-Vorschau
                        </summary>
                        <div
                          className="mt-2 bg-white border border-gray-200 rounded-lg p-4 text-sm"
                          dangerouslySetInnerHTML={{ __html: email.body_html }}
                        />
                      </details>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  )
}

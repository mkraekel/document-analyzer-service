import { useState, useMemo } from 'react'
import { Search, ChevronDown, ChevronUp, UserPlus, XCircle, Check } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { useToast } from '../hooks/useToast'
import { ToastContainer } from '../components/Toast'
import { Pagination } from '../components/Pagination'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { formatTime } from '../lib/format'
import { api } from '../api/client'
import type { TriageEmail, CaseListItem } from '../types/api'

const PER_PAGE = 15

export function Triage() {
  const { data: triageData, loading, refetch } = useApiGet<{ triage: TriageEmail[] }>('/api/dashboard/triage')
  const { data: casesData } = useApiGet<{ cases: CaseListItem[] }>('/api/dashboard/cases')
  const { toasts, addToast, removeToast } = useToast()
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [newCaseForm, setNewCaseForm] = useState<string | null>(null)
  const [newCaseName, setNewCaseName] = useState('')
  const [newCaseEmail, setNewCaseEmail] = useState('')
  const [assignCase, setAssignCase] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState<Set<string>>(new Set())

  const items = triageData?.triage || []
  const cases = casesData?.cases || []

  const filtered = useMemo(() => {
    if (!search.trim()) return items
    const q = search.toLowerCase()
    return items.filter(
      e =>
        (e.subject || '').toLowerCase().includes(q) ||
        (e.from_email || '').toLowerCase().includes(q) ||
        (e.body_text || '').toLowerCase().includes(q) ||
        JSON.stringify(e.parsed_result || {}).toLowerCase().includes(q),
    )
  }, [items, search])

  const totalPages = Math.ceil(filtered.length / PER_PAGE)
  const pageItems = filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE)

  function toggleExpand(id: string) {
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function markBusy(id: string, on: boolean) {
    setBusy(prev => {
      const next = new Set(prev)
      on ? next.add(id) : next.delete(id)
      return next
    })
  }

  async function handleAssign(msgId: string) {
    const caseId = assignCase[msgId]
    if (!caseId) return addToast('Bitte Case auswählen', 'error')
    markBusy(msgId, true)
    try {
      await api.post('/api/dashboard/assign', { provider_message_id: msgId, case_id: caseId })
      addToast('E-Mail zugeordnet', 'success')
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      markBusy(msgId, false)
    }
  }

  async function handleDismiss(msgId: string) {
    markBusy(msgId, true)
    try {
      await api.post('/api/dashboard/dismiss', { provider_message_id: msgId })
      addToast('E-Mail ignoriert', 'success')
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      markBusy(msgId, false)
    }
  }

  async function handleCreateCase(msgId: string) {
    if (!newCaseName.trim()) return addToast('Name ist Pflichtfeld', 'error')
    markBusy(msgId, true)
    try {
      const res = await api.post<{ case_id: string }>('/api/dashboard/create-case', {
        provider_message_id: msgId,
        applicant_name: newCaseName,
        partner_email: newCaseEmail,
      })
      addToast(`Case ${res.case_id} erstellt`, 'success')
      setNewCaseForm(null)
      setNewCaseName('')
      setNewCaseEmail('')
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      markBusy(msgId, false)
    }
  }

  if (loading) return <LoadingSpinner />

  return (
    <div>
      <ToastContainer toasts={toasts} onRemove={removeToast} />

      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Triage</h1>
          <p className="text-sm text-gray-500 mt-1">{filtered.length} E-Mails ohne Case-Zuordnung</p>
        </div>
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

      {pageItems.length === 0 ? (
        <div className="bg-white rounded-xl border border-gray-200 p-12 text-center text-gray-500">
          Keine E-Mails in der Triage-Queue
        </div>
      ) : (
        <div className="space-y-3">
          {pageItems.map(email => {
            const isExpanded = expanded.has(email.provider_message_id)
            const isBusy = busy.has(email.provider_message_id)
            const isNewCase = newCaseForm === email.provider_message_id
            const parsed = email.parsed_result || {}

            return (
              <div
                key={email.provider_message_id}
                className="bg-white rounded-xl border border-gray-200 overflow-hidden"
              >
                {/* Header */}
                <button
                  onClick={() => toggleExpand(email.provider_message_id)}
                  className="w-full px-5 py-4 flex items-start gap-4 text-left hover:bg-gray-50 transition-colors"
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-medium text-gray-900 truncate text-sm">
                        {email.subject || '(Kein Betreff)'}
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-xs text-gray-500">
                      <span>{email.from_email}</span>
                      <span title={email.processed_at}>{formatTime(email.processed_at)}</span>
                      {email.attachments_count > 0 && (
                        <span className="bg-gray-100 px-1.5 py-0.5 rounded">
                          {email.attachments_count} Anhänge
                        </span>
                      )}
                    </div>
                  </div>
                  {isExpanded ? <ChevronUp size={16} className="text-gray-400 mt-1" /> : <ChevronDown size={16} className="text-gray-400 mt-1" />}
                </button>

                {/* Expanded Content */}
                {isExpanded && (
                  <div className="px-5 pb-5 border-t border-gray-100">
                    {/* Body Preview */}
                    <div className="mt-3 mb-4 text-sm text-gray-600 bg-gray-50 rounded-lg p-3 max-h-48 overflow-y-auto whitespace-pre-wrap">
                      {email.body_text || '(Kein Text)'}
                    </div>

                    {/* Parsed Result */}
                    {Object.keys(parsed).length > 0 && (
                      <details className="mb-4">
                        <summary className="text-xs font-medium text-gray-500 cursor-pointer hover:text-gray-700">
                          GPT-Parsing Ergebnis
                        </summary>
                        <pre className="mt-2 text-xs bg-gray-50 rounded-lg p-3 overflow-x-auto max-h-48">
                          {JSON.stringify(parsed, null, 2)}
                        </pre>
                      </details>
                    )}

                    {/* Actions */}
                    <div className="flex flex-wrap gap-3 items-end">
                      {/* Assign to existing case */}
                      <div className="flex items-center gap-2">
                        <select
                          value={assignCase[email.provider_message_id] || ''}
                          onChange={e => setAssignCase(prev => ({ ...prev, [email.provider_message_id]: e.target.value }))}
                          className="text-sm border border-gray-300 rounded-lg px-3 py-2 bg-white"
                        >
                          <option value="">Case wählen...</option>
                          {cases.map(c => (
                            <option key={c.case_id} value={c.case_id}>
                              {c.applicant_name || c.case_id}
                            </option>
                          ))}
                        </select>
                        <button
                          onClick={() => handleAssign(email.provider_message_id)}
                          disabled={isBusy}
                          className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
                        >
                          <Check size={14} />
                          Zuordnen
                        </button>
                      </div>

                      {/* Dismiss */}
                      <button
                        onClick={() => handleDismiss(email.provider_message_id)}
                        disabled={isBusy}
                        className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
                      >
                        <XCircle size={14} />
                        Ignorieren
                      </button>

                      {/* New case toggle */}
                      <button
                        onClick={() => setNewCaseForm(isNewCase ? null : email.provider_message_id)}
                        className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-emerald-700 border border-emerald-300 rounded-lg hover:bg-emerald-50 transition-colors"
                      >
                        <UserPlus size={14} />
                        Neuer Case
                      </button>
                    </div>

                    {/* New Case Form */}
                    {isNewCase && (
                      <div className="mt-3 p-4 bg-emerald-50 rounded-lg border border-emerald-200 flex flex-wrap gap-3 items-end">
                        <div>
                          <label className="block text-xs font-medium text-gray-700 mb-1">Name *</label>
                          <input
                            type="text"
                            value={newCaseName}
                            onChange={e => setNewCaseName(e.target.value)}
                            className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-48"
                            placeholder="Max Mustermann"
                          />
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-gray-700 mb-1">Partner-E-Mail</label>
                          <input
                            type="email"
                            value={newCaseEmail}
                            onChange={e => setNewCaseEmail(e.target.value)}
                            className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-56"
                            placeholder="partner@example.de"
                          />
                        </div>
                        <button
                          onClick={() => handleCreateCase(email.provider_message_id)}
                          disabled={isBusy}
                          className="px-4 py-2 text-sm font-medium bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors"
                        >
                          Erstellen
                        </button>
                      </div>
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

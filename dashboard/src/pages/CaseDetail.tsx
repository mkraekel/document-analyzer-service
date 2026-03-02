import { useState, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft, ExternalLink, RefreshCw, Check, FileText,
  Mail, Clock, Shield, ChevronDown, ChevronUp, Pencil, X as XIcon, Save
} from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { useToast } from '../hooks/useToast'
import { ToastContainer } from '../components/Toast'
import { StatusBadge } from '../components/StatusBadge'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { formatTime, formatDateTime, fieldLabel, flattenObject } from '../lib/format'
import { api } from '../api/client'
import type { CaseDetail as CaseDetailType, CaseDocument } from '../types/api'

export function CaseDetail() {
  const { caseId } = useParams<{ caseId: string }>()
  const navigate = useNavigate()
  const { data: caseData, loading, refetch } = useApiGet<CaseDetailType>(
    `/api/dashboard/case/${caseId}`,
  )
  const { toasts, addToast, removeToast } = useToast()
  const [busy, setBusy] = useState(false)
  const [showAllDocs, setShowAllDocs] = useState(false)
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['readiness', 'financing', 'documents']),
  )

  const toggleSection = useCallback((key: string) => {
    setExpandedSections(prev => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })
  }, [])

  async function doAction(action: string) {
    setBusy(true)
    try {
      await api.post(`/api/dashboard/case/${caseId}/action`, { action })
      addToast(`Aktion "${action}" erfolgreich`, 'success')
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      setBusy(false)
    }
  }

  async function doOverride(key: string, value = 'true') {
    setBusy(true)
    try {
      await api.post(`/api/dashboard/case/${caseId}/override`, { key, value })
      addToast(`Override "${key}" gesetzt`, 'success')
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      setBusy(false)
    }
  }

  async function doImport(dryRun: boolean) {
    setBusy(true)
    try {
      const res = await api.post<{ europace_case_id?: string; errors?: string[] }>(
        `/api/dashboard/case/${caseId}/import`,
        { dry_run: dryRun },
      )
      if (res.europace_case_id) {
        addToast(`Import erfolgreich: ${res.europace_case_id}`, 'success')
      } else if (res.errors?.length) {
        addToast(`Import-Fehler: ${res.errors.join(', ')}`, 'error')
      } else {
        addToast(dryRun ? 'Dry-Run Import erfolgreich' : 'Import gestartet', 'success')
      }
      refetch()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      setBusy(false)
    }
  }

  async function scanDocuments() {
    setBusy(true)
    try {
      await api.post(`/api/dashboard/case/${caseId}/scan-documents`)
      addToast('Dokumente werden gescannt...', 'info')
      setTimeout(refetch, 3000)
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    } finally {
      setBusy(false)
    }
  }

  if (loading || !caseData) return <LoadingSpinner />

  const c = caseData
  const readiness = c.readiness || {}
  const facts = c.facts_extracted || {}
  const answers = c.answers_user || {}
  const overrides = c.manual_overrides || {}
  const flatFacts = flattenObject(facts)

  // Deduplicate documents
  const docMap = new Map<string, CaseDocument>()
  let dupeCount = 0
  for (const doc of c.documents || []) {
    const key = doc.file_name
    if (docMap.has(key)) {
      dupeCount++
    } else {
      docMap.set(key, doc)
    }
  }
  const uniqueDocs = Array.from(docMap.values())
  const visibleDocs = showAllDocs ? uniqueDocs : uniqueDocs.slice(0, 20)

  return (
    <div className="max-w-5xl">
      <ToastContainer toasts={toasts} onRemove={removeToast} />

      {/* Back + Header */}
      <button
        onClick={() => navigate('/app/cases')}
        className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-700 mb-4"
      >
        <ArrowLeft size={16} />
        Zurueck
      </button>

      <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 mb-1">
              {c.applicant_name || 'Unbekannt'}
            </h1>
            <div className="flex flex-wrap items-center gap-3 text-sm text-gray-500">
              <span className="font-mono text-xs">{c.case_id}</span>
              <StatusBadge status={c.status} />
              <span title={c.last_status_change}>
                <Clock size={12} className="inline mr-1" />
                {formatTime(c.last_status_change)}
              </span>
              {c.partner_email && <span>{c.partner_email}</span>}
            </div>
            {c.onedrive_web_url && (
              <a
                href={c.onedrive_web_url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800 mt-2"
              >
                <ExternalLink size={14} />
                OneDrive Ordner
              </a>
            )}
          </div>

          {/* Quick Actions */}
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => doAction('RECHECK')}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
            >
              <RefreshCw size={14} />
              Erneut pruefen
            </button>
            <button
              onClick={() => doAction('FREIGABE')}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 transition-colors"
            >
              <Check size={14} />
              Freigabe
            </button>
            {c.status === 'READY_FOR_IMPORT' && (
              <>
                <button
                  onClick={() => doImport(true)}
                  disabled={busy}
                  className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-blue-300 text-blue-700 rounded-lg hover:bg-blue-50 disabled:opacity-50 transition-colors"
                >
                  Dry-Run Import
                </button>
                <button
                  onClick={() => doImport(false)}
                  disabled={busy}
                  className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
                >
                  Import starten
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Readiness Checklist */}
      <Section
        title="Readiness Check"
        icon={<Shield size={18} />}
        isOpen={expandedSections.has('readiness')}
        onToggle={() => toggleSection('readiness')}
      >
        {/* Missing Financing */}
        {(readiness.missing_financing || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-orange-700 mb-2">Fehlende Finanzierungsdaten</h4>
            <div className="flex flex-wrap gap-2">
              {readiness.missing_financing.map((f: string) => (
                <span key={f} className="bg-orange-50 text-orange-700 text-xs px-2.5 py-1 rounded-full">
                  {fieldLabel(f)}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Missing Applicant Data */}
        {(readiness.missing_applicant_data || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-orange-700 mb-2">Fehlende Antragstellerdaten</h4>
            <div className="flex flex-wrap gap-2">
              {readiness.missing_applicant_data.map((f: string) => (
                <span key={f} className="bg-orange-50 text-orange-700 text-xs px-2.5 py-1 rounded-full">
                  {fieldLabel(f)}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Missing Docs */}
        {(readiness.missing_docs || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-red-700 mb-2">Fehlende Dokumente</h4>
            <div className="space-y-1.5">
              {readiness.missing_docs.map((d: { type: string; required: number; found: number }) => (
                <div key={d.type} className="flex items-center justify-between bg-red-50 rounded-lg px-3 py-2">
                  <span className="text-sm text-red-800">
                    {d.type} <span className="text-red-500">({d.found}/{d.required})</span>
                  </span>
                  <button
                    onClick={() => doOverride(`SKIP_DOC_${d.type}`, 'true')}
                    disabled={busy}
                    className="text-xs text-gray-500 hover:text-gray-700 disabled:opacity-50"
                  >
                    Nicht benoetigt
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Stale Docs */}
        {(readiness.stale_docs || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-amber-700 mb-2">Veraltete Dokumente</h4>
            <div className="space-y-1.5">
              {readiness.stale_docs.map((d: { type: string }) => (
                <div key={d.type} className="flex items-center justify-between bg-amber-50 rounded-lg px-3 py-2">
                  <span className="text-sm text-amber-800">{d.type}</span>
                  <button
                    onClick={() => doOverride(`ACCEPT_STALE_${d.type}`, 'true')}
                    disabled={busy}
                    className="text-xs text-gray-500 hover:text-gray-700 disabled:opacity-50"
                  >
                    Akzeptieren
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Overrides Applied */}
        {(readiness.manual_overrides_applied || []).length > 0 && (
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Aktive Overrides</h4>
            <div className="flex flex-wrap gap-2">
              {readiness.manual_overrides_applied.map((o: string) => (
                <span key={o} className="bg-purple-50 text-purple-700 text-xs px-2.5 py-1 rounded-full">
                  {o}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Recommended Missing */}
        {(readiness.recommended_missing || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-blue-700 mb-2">Empfohlene Daten (nicht blockierend)</h4>
            <div className="flex flex-wrap gap-2">
              {readiness.recommended_missing.map((f: string) => (
                <span key={f} className="bg-blue-50 text-blue-700 text-xs px-2.5 py-1 rounded-full">
                  {fieldLabel(f)}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* All good */}
        {!(readiness.missing_financing || []).length &&
         !(readiness.missing_applicant_data || []).length &&
         !(readiness.missing_docs || []).length &&
         !(readiness.stale_docs || []).length && (
          <p className="text-sm text-green-600 font-medium">Alle Checks bestanden</p>
        )}
      </Section>

      {/* Financing Data */}
      <Section
        title="Finanzierungsdaten"
        icon={<FileText size={18} />}
        isOpen={expandedSections.has('financing')}
        onToggle={() => toggleSection('financing')}
      >
        <EditableDataGrid
          caseId={caseId!}
          data={{
            purchase_price: flatFacts['property_data.purchase_price'] || flatFacts['purchase_price'] || '',
            loan_amount: flatFacts['financing_data.loan_amount'] || flatFacts['loan_amount'] || '',
            equity_to_use: flatFacts['financing_data.equity_to_use'] || flatFacts['equity_to_use'] || '',
            object_type: flatFacts['property_data.object_type'] || flatFacts['object_type'] || '',
            usage: flatFacts['property_data.usage'] || flatFacts['usage'] || '',
          }}
          target="facts"
          onSaved={refetch}
          addToast={addToast}
        />
      </Section>

      {/* Documents */}
      <Section
        title={`Dokumente (${uniqueDocs.length})`}
        icon={<FileText size={18} />}
        isOpen={expandedSections.has('documents')}
        onToggle={() => toggleSection('documents')}
        actions={
          c.onedrive_folder_id ? (
            <button
              onClick={scanDocuments}
              disabled={busy}
              className="text-xs text-blue-600 hover:text-blue-800 disabled:opacity-50"
            >
              Dokumente scannen
            </button>
          ) : undefined
        }
      >
        {dupeCount > 0 && (
          <p className="text-xs text-gray-500 mb-3">{dupeCount} Duplikate ausgeblendet</p>
        )}
        {visibleDocs.length === 0 ? (
          <p className="text-sm text-gray-500">Noch keine Dokumente</p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 px-3 text-xs font-medium text-gray-500">Datei</th>
                    <th className="text-left py-2 px-3 text-xs font-medium text-gray-500">Typ</th>
                    <th className="text-left py-2 px-3 text-xs font-medium text-gray-500">Status</th>
                    <th className="text-left py-2 px-3 text-xs font-medium text-gray-500">Datum</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleDocs.map((doc, i) => (
                    <tr key={i} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="py-2 px-3 text-gray-900 truncate max-w-[200px]">{doc.file_name}</td>
                      <td className="py-2 px-3 text-gray-600">{doc.doc_type || '-'}</td>
                      <td className="py-2 px-3">
                        <span className={`text-xs px-2 py-0.5 rounded-full ${
                          doc.processing_status === 'analyzed' ? 'bg-green-50 text-green-700' :
                          doc.processing_status === 'error' ? 'bg-red-50 text-red-700' :
                          'bg-gray-50 text-gray-600'
                        }`}>
                          {doc.processing_status || '-'}
                        </span>
                      </td>
                      <td className="py-2 px-3 text-gray-500 text-xs" title={doc.processed_at}>
                        {formatTime(doc.processed_at)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {!showAllDocs && uniqueDocs.length > 20 && (
              <button
                onClick={() => setShowAllDocs(true)}
                className="mt-3 text-sm text-blue-600 hover:text-blue-800"
              >
                Alle {uniqueDocs.length} Dokumente anzeigen
              </button>
            )}
          </>
        )}
      </Section>

      {/* Emails */}
      <Section
        title={`E-Mails (${(c.emails || []).length})`}
        icon={<Mail size={18} />}
        isOpen={expandedSections.has('emails')}
        onToggle={() => toggleSection('emails')}
      >
        {(c.emails || []).length === 0 ? (
          <p className="text-sm text-gray-500">Keine E-Mails</p>
        ) : (
          <div className="space-y-2">
            {c.emails.map((email, i) => (
              <div key={i} className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2">
                <div>
                  <span className="text-sm text-gray-900">{email.subject || '(Kein Betreff)'}</span>
                  <div className="text-xs text-gray-500">
                    {email.from_email} · {formatTime(email.processed_at)}
                  </div>
                </div>
                <span className={`text-xs px-2 py-0.5 rounded-full ${
                  email.processing_result === 'assigned' ? 'bg-green-50 text-green-700' :
                  email.processing_result === 'auto_matched' ? 'bg-blue-50 text-blue-700' :
                  'bg-gray-100 text-gray-600'
                }`}>
                  {email.processing_result}
                </span>
              </div>
            ))}
          </div>
        )}
      </Section>

      {/* Extracted Data */}
      <Section
        title="Extrahierte Daten"
        icon={<FileText size={18} />}
        isOpen={expandedSections.has('extracted')}
        onToggle={() => toggleSection('extracted')}
      >
        {Object.keys(flatFacts).length === 0 ? (
          <p className="text-sm text-gray-500">Keine Daten extrahiert</p>
        ) : (
          <EditableDataGrid
            caseId={caseId!}
            data={flatFacts}
            target="facts"
            onSaved={refetch}
            addToast={addToast}
          />
        )}
      </Section>

      {/* Answers & Overrides */}
      <Section
        title="Antworten &amp; Overrides"
        icon={<Shield size={18} />}
        isOpen={expandedSections.has('answers')}
        onToggle={() => toggleSection('answers')}
      >
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Antworten</h4>
            {Object.keys(answers).length === 0 ? (
              <p className="text-xs text-gray-500">Keine</p>
            ) : (
              <pre className="text-xs bg-gray-50 rounded-lg p-3 overflow-x-auto max-h-60">
                {JSON.stringify(answers, null, 2)}
              </pre>
            )}
          </div>
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Manuelle Overrides</h4>
            {Object.keys(overrides).length === 0 ? (
              <p className="text-xs text-gray-500">Keine</p>
            ) : (
              <pre className="text-xs bg-gray-50 rounded-lg p-3 overflow-x-auto max-h-60">
                {JSON.stringify(overrides, null, 2)}
              </pre>
            )}
          </div>
        </div>
      </Section>

      {/* Audit Log */}
      <Section
        title="Audit Log"
        icon={<Clock size={18} />}
        isOpen={expandedSections.has('audit')}
        onToggle={() => toggleSection('audit')}
      >
        {(c.audit_log || []).length === 0 ? (
          <p className="text-sm text-gray-500">Keine Eintraege</p>
        ) : (
          <div className="space-y-1">
            {c.audit_log.slice(0, 20).map((entry, i) => (
              <div key={i} className="flex items-center gap-3 text-xs py-1.5 border-b border-gray-50">
                <span className="text-gray-400 w-32 shrink-0">{formatDateTime(entry.ts)}</span>
                <span className="text-gray-700 font-medium">{entry.event}</span>
                {entry.status && <StatusBadge status={entry.status} />}
                {entry.source && <span className="text-gray-400">{entry.source}</span>}
              </div>
            ))}
          </div>
        )}
      </Section>
    </div>
  )
}

/* Collapsible Section Component */
function Section({
  title,
  icon,
  isOpen,
  onToggle,
  actions,
  children,
}: {
  title: string
  icon: React.ReactNode
  isOpen: boolean
  onToggle: () => void
  actions?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 mb-4 overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-5 py-4 hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-2 text-gray-900 font-medium text-sm">
          {icon}
          {title}
        </div>
        <div className="flex items-center gap-3">
          {actions && <div onClick={e => e.stopPropagation()}>{actions}</div>}
          {isOpen ? <ChevronUp size={16} className="text-gray-400" /> : <ChevronDown size={16} className="text-gray-400" />}
        </div>
      </button>
      {isOpen && <div className="px-5 pb-5">{children}</div>}
    </div>
  )
}

/* Editable Data Grid */
function EditableDataGrid({
  caseId,
  data,
  target,
  onSaved,
  addToast,
}: {
  caseId: string
  data: Record<string, string>
  target: string
  onSaved: () => void
  addToast: (msg: string, type: 'success' | 'error' | 'info') => void
}) {
  const [editing, setEditing] = useState<string | null>(null)
  const [editValue, setEditValue] = useState('')

  async function saveField(field: string) {
    try {
      await api.post(`/api/dashboard/case/${caseId}/update-field`, {
        field,
        value: editValue,
        target,
      })
      addToast(`${fieldLabel(field)} gespeichert`, 'success')
      setEditing(null)
      onSaved()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    }
  }

  return (
    <div className="grid gap-2">
      {Object.entries(data).map(([key, val]) => (
        <div key={key} className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2">
          <span className="text-xs font-medium text-gray-500 w-40 shrink-0">{fieldLabel(key)}</span>
          {editing === key ? (
            <div className="flex items-center gap-2 flex-1">
              <input
                type="text"
                value={editValue}
                onChange={e => setEditValue(e.target.value)}
                className="flex-1 text-sm border border-gray-300 rounded px-2 py-1"
                autoFocus
                onKeyDown={e => {
                  if (e.key === 'Enter') saveField(key)
                  if (e.key === 'Escape') setEditing(null)
                }}
              />
              <button onClick={() => saveField(key)} className="text-green-600 hover:text-green-800">
                <Save size={14} />
              </button>
              <button onClick={() => setEditing(null)} className="text-gray-400 hover:text-gray-600">
                <XIcon size={14} />
              </button>
            </div>
          ) : (
            <div className="flex items-center gap-2 flex-1 justify-end">
              <span className="text-sm text-gray-900">{val || '-'}</span>
              <button
                onClick={() => { setEditing(key); setEditValue(val || '') }}
                className="text-gray-300 hover:text-gray-500"
              >
                <Pencil size={12} />
              </button>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

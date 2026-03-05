import { useState, useCallback, useEffect, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft, ExternalLink, RefreshCw, Check, FileText,
  Mail, Clock, Shield, ChevronDown, ChevronUp, Pencil, X as XIcon, Save, Eye, Loader, Archive, XCircle
} from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { useToast } from '../hooks/useToast'
import { ToastContainer } from '../components/Toast'
import { StatusBadge } from '../components/StatusBadge'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { formatTime, formatDateTime, fieldLabel, flattenObject } from '../lib/format'
import { api } from '../api/client'
import type { CaseDetail as CaseDetailType, CaseDocument } from '../types/api'

function findValue(view: Record<string, unknown>, ...paths: string[]): string {
  for (const path of paths) {
    // Try flat key
    const flat = view[path]
    if (flat !== undefined && flat !== null && flat !== '' && typeof flat !== 'object') {
      return String(flat)
    }
    // Try nested path
    const parts = path.split('.')
    if (parts.length > 1) {
      let current: unknown = view
      for (const part of parts) {
        if (current && typeof current === 'object' && !Array.isArray(current)) {
          current = (current as Record<string, unknown>)[part]
        } else {
          current = undefined
          break
        }
      }
      if (current !== undefined && current !== null && current !== '' && typeof current !== 'object') {
        return String(current)
      }
    }
  }
  return ''
}

const EUROPACE_GROUPS = [
  {
    title: 'Antragsteller',
    fields: [
      { key: 'salutation', paths: ['salutation'] },
      { key: 'applicant_first_name', paths: ['applicant_first_name', 'applicant_data.first_name'] },
      { key: 'applicant_last_name', paths: ['applicant_last_name', 'applicant_data.last_name'] },
      { key: 'applicant_birth_date', paths: ['applicant_birth_date', 'applicant_data.birth_date'] },
      { key: 'birth_place', paths: ['birth_place', 'applicant_data.birth_place'] },
      { key: 'nationality', paths: ['nationality', 'applicant_data.nationality'] },
      { key: 'tax_id', paths: ['tax_id', 'applicant_data.tax_id'] },
      { key: 'phone', paths: ['phone', 'applicant_data.phone'] },
    ],
  },
  {
    title: 'Wohnadresse',
    fields: [
      { key: 'address_street', paths: ['address_street', 'address_data.street'] },
      { key: 'address_house_number', paths: ['address_house_number', 'address_data.house_number'] },
      { key: 'address_zip', paths: ['address_zip', 'address_data.zip'] },
      { key: 'address_city', paths: ['address_city', 'address_data.city'] },
    ],
  },
  {
    title: 'Beschäftigung & Einkommen',
    fields: [
      { key: 'employment_type', paths: ['employment_type', 'applicant_data.employment_type'] },
      { key: 'occupation', paths: ['occupation', 'applicant_data.occupation'] },
      { key: 'employer', paths: ['employer', 'applicant_data.employer'] },
      { key: 'employed_since', paths: ['employed_since', 'applicant_data.employed_since'] },
      { key: 'net_income', paths: ['net_income', 'applicant_data.net_income'] },
      { key: 'monthly_rental_income', paths: ['monthly_rental_income'] },
    ],
  },
  {
    title: 'Objekt',
    fields: [
      { key: 'object_type', paths: ['object_type', 'property_data.object_type'] },
      { key: 'usage', paths: ['usage', 'property_data.usage'] },
      { key: 'property_street', paths: ['property_street', 'property_data.street'] },
      { key: 'property_zip', paths: ['property_zip', 'property_data.zip'] },
      { key: 'property_city', paths: ['property_city', 'property_data.city'] },
      { key: 'living_space', paths: ['living_space', 'property_data.living_space'] },
      { key: 'year_built', paths: ['year_built', 'property_data.year_built'] },
    ],
  },
  {
    title: 'Finanzierung',
    fields: [
      { key: 'purchase_price', paths: ['purchase_price', 'property_data.purchase_price'] },
      { key: 'loan_amount', paths: ['loan_amount', 'financing_data.loan_amount'] },
      { key: 'equity_to_use', paths: ['equity_to_use', 'financing_data.equity_to_use'] },
      { key: 'zinsbindung', paths: ['zinsbindung'] },
      { key: 'wunschrate', paths: ['wunschrate'] },
    ],
  },
]

const PARTNER_OPTIONS = [
  { id: 'CZU26', label: 'Alexander Heil (CZU26)' },
  { id: 'XET70', label: 'Matthias Lächele (XET70)' },
]

export function CaseDetail() {
  const { caseId } = useParams<{ caseId: string }>()
  const navigate = useNavigate()
  const { data: caseData, loading, refetch } = useApiGet<CaseDetailType>(
    `/api/dashboard/case/${caseId}`,
  )
  const { toasts, addToast, removeToast } = useToast()
  const [busy, setBusy] = useState(false)
  const [showAllDocs, setShowAllDocs] = useState(false)

  // ── Processing Queue Polling ──
  interface QueueItem {
    filename: string
    status: 'queued' | 'processing' | 'done' | 'error'
    queued_at: string
    started_at: string | null
    finished_at: string | null
    doc_type: string | null
    error: string | null
  }
  interface QueueStatus {
    active: QueueItem[]
    recent: QueueItem[]
    total_queued: number
    total_processing: number
    total_done: number
    total_error: number
  }
  const [queue, setQueue] = useState<QueueStatus | null>(null)

  const prevActiveCount = useRef<number | null>(null)
  useEffect(() => {
    if (!caseId) return
    let active = true
    const poll = async () => {
      try {
        const q = await api.get<QueueStatus>(`/api/dashboard/case/${caseId}/queue`)
        if (!active) return
        setQueue(q)
        // Only refetch case data when processing just finished (active → 0)
        const wasProcessing = prevActiveCount.current !== null && prevActiveCount.current > 0
        prevActiveCount.current = q.active.length
        if (wasProcessing && q.active.length === 0) {
          refetch()
        }
      } catch { /* ignore */ }
    }
    poll()
    const interval = setInterval(poll, 3000)
    return () => { active = false; clearInterval(interval) }
  }, [caseId])
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

  useEffect(() => {
    if (caseData?.status === 'READY_FOR_IMPORT') {
      setExpandedSections(prev => new Set([...prev, 'europace']))
    }
  }, [caseData?.status])

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

  async function doReanalyze() {
    if (!confirm('Alle Dokumente werden neu analysiert. Extrahierte Daten werden zurückgesetzt. Fortfahren?')) return
    setBusy(true)
    try {
      await api.post(`/api/dashboard/case/${caseId}/action`, { action: 'REANALYZE' })
      addToast('Neuanalyse gestartet – läuft im Hintergrund', 'info')
      // Regelmäßig refreshen um Fortschritt zu zeigen
      const interval = setInterval(refetch, 10000)
      setTimeout(() => clearInterval(interval), 120000)
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler bei Neuanalyse', 'error')
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
  const effectiveView = (readiness.effective_view || {}) as Record<string, unknown>
  const missingFields = new Set([
    ...(readiness.missing_financing || []),
    ...(readiness.missing_applicant_data || []),
  ])

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
        Zurück
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
            <div className="flex flex-wrap gap-3 mt-2">
              {c.onedrive_web_url && (
                <a
                  href={c.onedrive_web_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800"
                >
                  <ExternalLink size={14} />
                  OneDrive Ordner
                </a>
              )}
              {(c.google_drive_links || []).map((link, i) => (
                <a
                  key={i}
                  href={link}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-sm text-green-600 hover:text-green-800"
                >
                  <ExternalLink size={14} />
                  Google Drive{(c.google_drive_links || []).length > 1 ? ` (${i + 1})` : ''}
                </a>
              ))}
            </div>
          </div>

          {/* Quick Actions */}
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => doAction('RECHECK')}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
            >
              <RefreshCw size={14} />
              Erneut prüfen
            </button>
            <button
              onClick={doReanalyze}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-orange-300 text-orange-700 rounded-lg hover:bg-orange-50 disabled:opacity-50 transition-colors"
            >
              <RefreshCw size={14} />
              Neu analysieren
            </button>
            <button
              onClick={() => doAction('FREIGABE')}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 transition-colors"
            >
              <Check size={14} />
              Freigabe
            </button>
            <button
              onClick={() => { if (confirm('Case wirklich ablehnen?')) doAction('DECLINE') }}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-red-300 text-red-700 rounded-lg hover:bg-red-50 disabled:opacity-50 transition-colors"
            >
              <XCircle size={14} />
              Ablehnen
            </button>
            <button
              onClick={() => { if (confirm('Case archivieren?')) doAction('ARCHIVE') }}
              disabled={busy}
              className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-gray-300 text-gray-600 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
            >
              <Archive size={14} />
              Archivieren
            </button>
            {c.status === 'READY_FOR_IMPORT' && (
              <>
                <button
                  disabled
                  title="Europace API-Key noch nicht konfiguriert"
                  className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium border border-gray-200 text-gray-400 rounded-lg cursor-not-allowed"
                >
                  Dry-Run Import
                </button>
                <button
                  disabled
                  title="Europace API-Key noch nicht konfiguriert"
                  className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-gray-300 text-gray-500 rounded-lg cursor-not-allowed"
                >
                  Import starten
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Investagon Banner */}
      {(c.investagon_links || []).length > 0 && (
        <div className="bg-amber-50 border border-amber-300 rounded-xl px-4 py-3 mb-4 shadow-sm">
          <div className="flex items-start gap-3">
            <span className="text-amber-600 text-lg mt-0.5">&#9888;</span>
            <div className="flex-1">
              <h3 className="text-sm font-semibold text-amber-900">Manuelle Aktion erforderlich</h3>
              <p className="text-sm text-amber-800 mt-1">
                Investagon-Links gefunden — Daten manuell abrufen und unten in die Felder eintragen:
              </p>
              <div className="mt-2 flex flex-wrap gap-2">
                {c.investagon_links.map((link, i) => (
                  <a
                    key={i}
                    href={link}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 px-3 py-1.5 text-sm font-medium bg-amber-100 text-amber-900 rounded-lg hover:bg-amber-200 transition-colors"
                  >
                    <ExternalLink size={14} />
                    Investagon{c.investagon_links.length > 1 ? ` (${i + 1})` : ''} öffnen
                  </a>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

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
              {(readiness.missing_applicant_data ?? []).map((f: string) => (
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
                    Nicht benötigt
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

        {/* Warnings */}
        {(readiness.warnings || []).length > 0 && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-amber-700 mb-2">Hinweise</h4>
            <div className="space-y-1.5">
              {(readiness.warnings as string[]).map((w: string, i: number) => (
                <div key={i} className="bg-amber-50 text-sm text-amber-800 rounded-lg px-3 py-2">
                  {w}
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
              {(readiness.recommended_missing ?? []).map((f: string) => (
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
            purchase_price: findValue(effectiveView, 'purchase_price', 'property_data.purchase_price') || '',
            loan_amount: findValue(effectiveView, 'loan_amount', 'financing_data.loan_amount') || '',
            equity_to_use: findValue(effectiveView, 'equity_to_use', 'financing_data.equity_to_use') || '',
            object_type: findValue(effectiveView, 'object_type', 'property_data.object_type') || '',
            usage: findValue(effectiveView, 'usage', 'property_data.usage') || '',
          }}
          target="overrides"
          onSaved={refetch}
          addToast={addToast}
        />
      </Section>

      {/* Europace Preview */}
      <Section
        title="Europace-Vorschau"
        icon={<Eye size={18} />}
        isOpen={expandedSections.has('europace')}
        onToggle={() => toggleSection('europace')}
      >
        {EUROPACE_GROUPS.map(group => (
          <div key={group.title} className="mb-5 last:mb-0">
            <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
              {group.title}
            </h4>
            <div className="grid gap-1">
              {group.fields.map(field => {
                const val = findValue(effectiveView, ...field.paths)
                const isMissing = missingFields.has(field.key)
                return (
                  <div key={field.key} className="flex items-center justify-between py-1.5 px-3 rounded bg-gray-50">
                    <span className="text-xs font-medium text-gray-500">{fieldLabel(field.key)}</span>
                    <span className={`text-sm ${
                      val
                        ? 'text-gray-900'
                        : isMissing
                          ? 'text-orange-500 font-medium'
                          : 'text-gray-300'
                    }`}>
                      {val || '–'}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        ))}
        {/* Vermittler Dropdowns */}
        <div className="mb-5 last:mb-0">
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
            Vermittler
          </h4>
          <div className="grid gap-1">
            <PartnerSelect
              label="Partner-ID"
              value={findValue(effectiveView, 'partnerId')}
              caseId={caseId!}
              fieldKey="partnerId"
              onSaved={refetch}
              addToast={addToast}
            />
          </div>
        </div>
      </Section>

      {/* Processing Queue */}
      {queue && (queue.active.length > 0 || queue.recent.length > 0) && (
        <div className="bg-white rounded-xl border border-blue-200 shadow-sm mb-4">
          <div className="px-4 py-3 border-b border-blue-100 flex items-center gap-2">
            {queue.active.length > 0 && <Loader size={16} className="animate-spin text-blue-500" />}
            <h3 className="text-sm font-semibold text-blue-900">
              Verarbeitungs-Queue
              {queue.active.length > 0 && (
                <span className="ml-2 text-xs font-normal text-blue-600">
                  {queue.total_processing > 0 && `${queue.total_processing} in Analyse`}
                  {queue.total_processing > 0 && queue.total_queued > 0 && ', '}
                  {queue.total_queued > 0 && `${queue.total_queued} wartend`}
                </span>
              )}
            </h3>
          </div>
          <div className="px-4 py-2 space-y-1 max-h-[300px] overflow-y-auto">
            {[...queue.active, ...queue.recent].map((item, i) => (
              <div key={`${item.filename}-${i}`} className="flex items-center gap-2 py-1.5 text-sm border-b border-gray-50 last:border-0">
                {item.status === 'queued' && (
                  <span className="w-2 h-2 rounded-full bg-gray-300 flex-shrink-0" title="Wartend" />
                )}
                {item.status === 'processing' && (
                  <Loader size={12} className="animate-spin text-blue-500 flex-shrink-0" />
                )}
                {item.status === 'done' && (
                  <Check size={12} className="text-green-500 flex-shrink-0" />
                )}
                {item.status === 'error' && (
                  <XIcon size={12} className="text-red-500 flex-shrink-0" />
                )}
                <span className="truncate text-gray-700 flex-1" title={item.filename}>
                  {item.filename}
                </span>
                {item.doc_type && (
                  <span className="text-xs text-gray-400 flex-shrink-0">{item.doc_type}</span>
                )}
                {item.error && (
                  <span className="text-xs text-red-500 truncate max-w-[200px]" title={item.error}>
                    {item.error}
                  </span>
                )}
                <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${
                  item.status === 'queued' ? 'bg-gray-100 text-gray-500' :
                  item.status === 'processing' ? 'bg-blue-50 text-blue-600' :
                  item.status === 'done' ? 'bg-green-50 text-green-600' :
                  'bg-red-50 text-red-600'
                }`}>
                  {item.status === 'queued' ? 'wartend' :
                   item.status === 'processing' ? 'analysiert...' :
                   item.status === 'done' ? 'fertig' : 'Fehler'}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Documents */}
      <Section
        title={`Dokumente (${uniqueDocs.length})`}
        icon={<FileText size={18} />}
        isOpen={expandedSections.has('documents')}
        onToggle={() => toggleSection('documents')}
        actions={
          c.onedrive_folder_id ? (
            <button
              disabled
              title="Scan-Webhook noch nicht konfiguriert"
              className="text-xs text-gray-400 cursor-not-allowed"
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
                  {visibleDocs.map((doc, i) => {
                    const displayName = doc.file_name?.startsWith('gdrive:') ? doc.file_name.slice(7) : doc.file_name
                    return (
                    <tr key={i} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="py-2 px-3 text-gray-900 truncate max-w-[200px]" title={displayName}>
                        {displayName}
                      </td>
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
                      <td className="py-2 px-3 text-gray-500 text-xs">
                        <span className="flex items-center gap-1.5">
                          <span title={doc.processed_at}>{formatTime(doc.processed_at)}</span>
                          {doc.gdrive_url && (
                            <a href={doc.gdrive_url} target="_blank" rel="noopener noreferrer" title="In Google Drive öffnen" className="text-blue-500 hover:text-blue-700 flex-shrink-0">
                              <ExternalLink size={13} />
                            </a>
                          )}
                          {!doc.gdrive_url && doc.onedrive_url && (
                            <a href={doc.onedrive_url} target="_blank" rel="noopener noreferrer" title="In OneDrive öffnen" className="text-blue-500 hover:text-blue-700 flex-shrink-0">
                              <ExternalLink size={13} />
                            </a>
                          )}
                        </span>
                      </td>
                    </tr>
                  )})}
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
            {c.emails.map((email, i) => {
              const parsed = email.parsed_result || {}
              return (
                <details key={i} className="bg-gray-50 rounded-lg overflow-hidden">
                  <summary className="flex items-center justify-between px-3 py-2 cursor-pointer hover:bg-gray-100 transition-colors">
                    <div className="flex-1 min-w-0">
                      <span className="text-sm text-gray-900">{email.subject || '(Kein Betreff)'}</span>
                      <div className="text-xs text-gray-500">
                        {email.from_email} · {formatTime(email.processed_at)}
                        {email.mail_type && <span> · {email.mail_type}</span>}
                      </div>
                    </div>
                    <span className={`text-xs px-2 py-0.5 rounded-full shrink-0 ml-2 ${
                      email.processing_result === 'assigned' ? 'bg-green-50 text-green-700' :
                      email.processing_result === 'auto_matched' ? 'bg-blue-50 text-blue-700' :
                      'bg-gray-100 text-gray-600'
                    }`}>
                      {email.processing_result}
                    </span>
                  </summary>
                  <div className="px-3 pb-3 border-t border-gray-200">
                    {email.body_html ? (
                      <iframe
                        srcDoc={email.body_html}
                        sandbox=""
                        className="mt-2 w-full bg-white rounded-lg border border-gray-200"
                        style={{ minHeight: '200px', maxHeight: '500px' }}
                        onLoad={(e) => {
                          const f = e.currentTarget
                          if (f.contentDocument?.body) {
                            f.style.height = Math.min(f.contentDocument.body.scrollHeight + 20, 500) + 'px'
                          }
                        }}
                      />
                    ) : (
                      <div className="mt-2 text-sm text-gray-700 bg-white rounded-lg p-3 max-h-96 overflow-y-auto whitespace-pre-wrap">
                        {email.body_text || '(Kein Text)'}
                      </div>
                    )}
                    {Object.keys(parsed).length > 0 && (
                      <details className="mt-2">
                        <summary className="text-xs font-medium text-gray-500 cursor-pointer hover:text-gray-700">
                          Parsing-Ergebnis
                        </summary>
                        <pre className="mt-1 text-xs bg-white rounded-lg p-3 overflow-x-auto max-h-48">
                          {JSON.stringify(parsed, null, 2)}
                        </pre>
                      </details>
                    )}
                  </div>
                </details>
              )
            })}
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
          <p className="text-sm text-gray-500">Keine Einträge</p>
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

/* Partner-ID Dropdown */
function PartnerSelect({
  label,
  value,
  caseId,
  fieldKey,
  onSaved,
  addToast,
}: {
  label: string
  value: string
  caseId: string
  fieldKey: string
  onSaved: () => void
  addToast: (msg: string, type: 'success' | 'error' | 'info') => void
}) {
  async function handleChange(newValue: string) {
    try {
      await api.post(`/api/dashboard/case/${caseId}/update-field`, {
        field: fieldKey,
        value: newValue,
        target: 'overrides',
      })
      addToast(`${label} gespeichert`, 'success')
      onSaved()
    } catch (e) {
      addToast(e instanceof Error ? e.message : 'Fehler', 'error')
    }
  }

  return (
    <div className="flex items-center justify-between py-1.5 px-3 rounded bg-gray-50">
      <span className="text-xs font-medium text-gray-500">{label}</span>
      <select
        value={value || ''}
        onChange={e => handleChange(e.target.value)}
        className="text-sm text-gray-900 bg-white border border-gray-300 rounded px-2 py-1 cursor-pointer hover:border-gray-400 focus:outline-none focus:ring-1 focus:ring-blue-500"
      >
        <option value="">– Auswählen –</option>
        {PARTNER_OPTIONS.map(opt => (
          <option key={opt.id} value={opt.id}>{opt.label}</option>
        ))}
      </select>
    </div>
  )
}

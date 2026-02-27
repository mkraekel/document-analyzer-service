const STATUS_LABELS: Record<string, string> = {
  INTAKE: 'Eingang',
  WAITING_FOR_DOCUMENTS: 'Warte auf Dokumente',
  NEEDS_QUESTIONS_PARTNER: 'Rueckfragen Partner',
  NEEDS_QUESTIONS_BROKER: 'Rueckfragen Broker',
  NEEDS_MANUAL_REVIEW_BROKER: 'Manuelle Pruefung',
  AWAITING_BROKER_CONFIRMATION: 'Warte auf Bestaetigung',
  READY_FOR_IMPORT: 'Bereit fuer Import',
  IMPORTED: 'Importiert',
  ERROR: 'Fehler',
  ARCHIVED: 'Archiviert',
}

const STATUS_COLORS: Record<string, string> = {
  INTAKE: 'bg-gray-100 text-gray-700',
  WAITING_FOR_DOCUMENTS: 'bg-yellow-100 text-yellow-800',
  NEEDS_QUESTIONS_PARTNER: 'bg-orange-100 text-orange-800',
  NEEDS_QUESTIONS_BROKER: 'bg-orange-100 text-orange-800',
  NEEDS_MANUAL_REVIEW_BROKER: 'bg-red-100 text-red-800',
  AWAITING_BROKER_CONFIRMATION: 'bg-blue-100 text-blue-800',
  READY_FOR_IMPORT: 'bg-green-100 text-green-800',
  IMPORTED: 'bg-emerald-100 text-emerald-800',
  ERROR: 'bg-red-200 text-red-900',
  ARCHIVED: 'bg-gray-200 text-gray-600',
}

export function StatusBadge({ status }: { status: string }) {
  const label = STATUS_LABELS[status] || status
  const color = STATUS_COLORS[status] || 'bg-gray-100 text-gray-700'
  return (
    <span
      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}
      title={status}
    >
      {label}
    </span>
  )
}

export { STATUS_LABELS }

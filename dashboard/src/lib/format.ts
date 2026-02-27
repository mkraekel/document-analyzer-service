export function formatTime(iso: string | null | undefined): string {
  if (!iso) return '-'
  try {
    const d = new Date(iso)
    const now = new Date()
    const diff = Math.floor((now.getTime() - d.getTime()) / 1000)
    if (diff < 60) return 'gerade eben'
    if (diff < 3600) return `vor ${Math.floor(diff / 60)} Min.`
    if (diff < 86400) return `vor ${Math.floor(diff / 3600)} Std.`
    if (diff < 604800) return `vor ${Math.floor(diff / 86400)} Tagen`
    return d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' })
  } catch {
    return iso
  }
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return '-'
  try {
    return new Date(iso).toLocaleString('de-DE', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return iso
  }
}

export function esc(str: string | null | undefined): string {
  return str || ''
}

export function flattenObject(
  obj: Record<string, unknown>,
  prefix = '',
): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [key, val] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(result, flattenObject(val as Record<string, unknown>, fullKey))
    } else if (val !== null && val !== undefined && val !== '') {
      result[fullKey] = String(val)
    }
  }
  return result
}

const FIELD_LABELS: Record<string, string> = {
  purchase_price: 'Kaufpreis',
  loan_amount: 'Darlehenssumme',
  equity_to_use: 'Eigenkapital',
  object_type: 'Objektart',
  usage: 'Nutzungsart',
  employment_type: 'Beschaeftigungsart',
  applicant_name: 'Antragsteller',
  partner_email: 'Partner E-Mail',
  'property_data.purchase_price': 'Kaufpreis',
  'property_data.object_type': 'Objektart',
  'property_data.usage': 'Nutzungsart',
  'financing_data.loan_amount': 'Darlehenssumme',
  'financing_data.equity_to_use': 'Eigenkapital',
}

export function fieldLabel(key: string): string {
  return FIELD_LABELS[key] || key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
}

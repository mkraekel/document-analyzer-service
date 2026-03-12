import { useState } from 'react'
import { Trash2, Plus } from 'lucide-react'
import { useApiGet } from '../hooks/useApi'
import { api } from '../api/client'
import { LoadingSpinner } from '../components/LoadingSpinner'
import type { Partner } from '../types/api'

export function Partners() {
  const { data, loading, refetch } = useApiGet<{ partners: Partner[] }>('/api/partners')
  const [email, setEmail] = useState('')
  const [name, setName] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const partners = data?.partners || []

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault()
    if (!email.trim()) return
    setSubmitting(true)
    setError(null)
    try {
      await api.post('/api/partners', { email: email.trim(), name: name.trim() })
      setEmail('')
      setName('')
      refetch()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Fehler beim Hinzufügen')
    } finally {
      setSubmitting(false)
    }
  }

  async function handleDelete(id: string, partnerEmail: string) {
    if (!confirm(`Partner "${partnerEmail}" wirklich entfernen?`)) return
    try {
      await api.delete(`/api/partners/${id}`)
      refetch()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Fehler beim Löschen')
    }
  }

  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Vertriebspartner</h1>
        <p className="text-sm text-gray-500 mt-1">
          Nur E-Mails von freigeschalteten Partnern werden als Anfragen verarbeitet.
        </p>
      </div>

      {/* Add form */}
      <form onSubmit={handleAdd} className="bg-white rounded-xl border border-gray-200 p-5 mb-6 flex flex-wrap gap-3 items-end">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-xs font-medium text-gray-500 mb-1">E-Mail *</label>
          <input
            type="email"
            value={email}
            onChange={e => setEmail(e.target.value)}
            required
            className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="partner@firma.de"
          />
        </div>
        <div className="flex-1 min-w-[150px]">
          <label className="block text-xs font-medium text-gray-500 mb-1">Name / Firma</label>
          <input
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="Optional"
          />
        </div>
        <button
          type="submit"
          disabled={submitting}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          <Plus size={16} />
          Hinzufügen
        </button>
      </form>

      {error && (
        <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm">
          {error}
        </div>
      )}

      {/* Partner list */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100 bg-gray-50">
          <span className="text-xs font-medium text-gray-500 uppercase">
            {partners.length} Partner freigeschaltet
          </span>
        </div>
        {partners.length === 0 ? (
          <div className="p-12 text-center text-gray-500">
            Noch keine Partner angelegt.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead className="bg-gray-50 text-gray-500 text-xs uppercase">
              <tr>
                <th className="px-5 py-3 text-left">E-Mail</th>
                <th className="px-5 py-3 text-left hidden sm:table-cell">Name / Firma</th>
                <th className="px-5 py-3 text-right w-16"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {partners.map(p => (
                <tr key={p._id} className="hover:bg-gray-50">
                  <td className="px-5 py-3 font-medium text-gray-900">{p.email}</td>
                  <td className="px-5 py-3 text-gray-600 hidden sm:table-cell">{p.name || '-'}</td>
                  <td className="px-5 py-3 text-right">
                    <button
                      onClick={() => handleDelete(p._id, p.email)}
                      className="p-1.5 text-gray-400 hover:text-red-600 rounded-lg hover:bg-red-50 transition-colors"
                      title="Partner entfernen"
                    >
                      <Trash2 size={16} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

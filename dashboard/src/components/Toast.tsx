import { X } from 'lucide-react'
import type { Toast as ToastType } from '../hooks/useToast'

const colors = {
  success: 'bg-green-500',
  error: 'bg-red-500',
  info: 'bg-blue-500',
}

export function ToastContainer({
  toasts,
  onRemove,
}: {
  toasts: ToastType[]
  onRemove: (id: number) => void
}) {
  if (toasts.length === 0) return null
  return (
    <div className="fixed top-4 right-4 z-50 flex flex-col gap-2">
      {toasts.map(t => (
        <div
          key={t.id}
          className={`${colors[t.type]} text-white px-4 py-3 rounded-lg shadow-lg flex items-center gap-3 min-w-72 animate-slide-in`}
        >
          <span className="flex-1 text-sm">{t.message}</span>
          <button onClick={() => onRemove(t.id)} className="opacity-70 hover:opacity-100">
            <X size={16} />
          </button>
        </div>
      ))}
    </div>
  )
}

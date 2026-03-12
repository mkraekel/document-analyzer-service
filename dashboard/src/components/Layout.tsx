import { NavLink, Outlet } from 'react-router-dom'
import { LayoutDashboard, Inbox, Briefcase, Mail, Users, LogOut, Menu, X, Zap } from 'lucide-react'
import { useState, useEffect } from 'react'
import { useAuth } from '../context/AuthContext'
import { api } from '../api/client'
import type { OpenAICredits, DashboardStats } from '../types/api'

const navItems = [
  { to: '/app', label: 'Übersicht', icon: LayoutDashboard, end: true },
  { to: '/app/triage', label: 'Triage', icon: Inbox, end: false },
  { to: '/app/cases', label: 'Cases', icon: Briefcase, end: false },
  { to: '/app/emails', label: 'Ausgehende Mails', icon: Mail, end: false },
  { to: '/app/partners', label: 'Partner', icon: Users, end: false },
]

function OpenAICreditsDisplay() {
  const [credits, setCredits] = useState<OpenAICredits | null>(null)

  useEffect(() => {
    const fetchCredits = async () => {
      try {
        const data = await api.get<OpenAICredits>('/api/dashboard/openai-credits')
        setCredits(data)
      } catch {
        // silently ignore
      }
    }
    fetchCredits()
    const interval = setInterval(fetchCredits, 3600_000) // refresh every hour
    return () => clearInterval(interval)
  }, [])

  if (!credits || credits.error) return null

  // Show either: monthly usage or remaining credit balance
  const used = credits.used_usd
  const limit = credits.hard_limit_usd
  const available = credits.total_available

  // Nothing useful to show
  if (used == null && available == null) return null

  const pct = limit && used != null ? Math.round((used / limit) * 100) : null
  const isWarning = pct != null && pct > 80

  return (
    <div
      className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-gray-100 text-xs font-medium text-gray-600"
      title={[
        used != null ? `Verbrauch diesen Monat: $${used.toFixed(2)}` : '',
        limit ? `Limit: $${limit}` : '',
        available != null ? `Guthaben: $${available.toFixed(2)}` : '',
      ].filter(Boolean).join(' · ')}
    >
      <Zap size={12} className={isWarning ? 'text-red-500' : 'text-amber-500'} />
      {used != null && <span>${used.toFixed(2)}{limit ? <span className="text-gray-400"> / ${limit}</span> : null}</span>}
      {available != null && <span className="text-green-600">${available.toFixed(2)} left</span>}
    </div>
  )
}

export function Layout() {
  const { user, logout } = useAuth()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [errorCount, setErrorCount] = useState(0)

  useEffect(() => {
    const fetchErrors = async () => {
      try {
        const data = await api.get<DashboardStats>('/api/dashboard/stats')
        setErrorCount(data.errors_24h || 0)
      } catch {
        // silently ignore
      }
    }
    fetchErrors()
    const interval = setInterval(fetchErrors, 60_000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/30 z-30 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed lg:static inset-y-0 left-0 z-40 w-64 bg-white border-r border-gray-200 flex flex-col transform transition-transform lg:translate-x-0 ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="px-6 py-5 border-b border-gray-100">
          <h1 className="text-lg font-bold text-gray-900">Finanzierung</h1>
          <p className="text-xs text-gray-500 mt-0.5">Alexander Heil Consulting</p>
        </div>

        <nav className="flex-1 px-3 py-4 space-y-1">
          {navItems.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              onClick={() => setSidebarOpen(false)}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                }`
              }
            >
              <item.icon size={18} />
              {item.label}
              {item.to === '/app' && errorCount > 0 && (
                <span className="ml-auto bg-red-500 text-white text-xs font-bold rounded-full w-5 h-5 flex items-center justify-center">
                  {errorCount > 99 ? '99+' : errorCount}
                </span>
              )}
            </NavLink>
          ))}
        </nav>

        <div className="px-3 py-4 border-t border-gray-100">
          <div className="px-3 py-2 text-xs text-gray-500 truncate">{user}</div>
          <button
            onClick={logout}
            className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-gray-600 hover:bg-gray-50 hover:text-gray-900 w-full transition-colors"
          >
            <LogOut size={18} />
            Abmelden
          </button>
        </div>
      </aside>

      {/* Main */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="flex items-center justify-between px-4 py-3 bg-white border-b border-gray-200">
          <div className="flex items-center gap-3">
            <button onClick={() => setSidebarOpen(true)} className="p-1 lg:hidden">
              {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
            </button>
            <h1 className="text-lg font-bold text-gray-900 lg:hidden">Finanzierung</h1>
          </div>
          <OpenAICreditsDisplay />
        </header>

        <main className="flex-1 overflow-y-auto p-4 lg:p-8">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

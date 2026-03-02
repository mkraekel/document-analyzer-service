import { NavLink, Outlet } from 'react-router-dom'
import { LayoutDashboard, Inbox, Briefcase, Mail, LogOut, Menu, X } from 'lucide-react'
import { useState } from 'react'
import { useAuth } from '../context/AuthContext'

const navItems = [
  { to: '/app', label: 'Übersicht', icon: LayoutDashboard, end: true },
  { to: '/app/triage', label: 'Triage', icon: Inbox, end: false },
  { to: '/app/cases', label: 'Cases', icon: Briefcase, end: false },
  { to: '/app/emails', label: 'Ausgehende Mails', icon: Mail, end: false },
]

export function Layout() {
  const { user, logout } = useAuth()
  const [sidebarOpen, setSidebarOpen] = useState(false)

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
        {/* Top bar (mobile) */}
        <header className="lg:hidden flex items-center gap-3 px-4 py-3 bg-white border-b border-gray-200">
          <button onClick={() => setSidebarOpen(true)} className="p-1">
            {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
          </button>
          <h1 className="text-lg font-bold text-gray-900">Finanzierung</h1>
        </header>

        <main className="flex-1 overflow-y-auto p-4 lg:p-8">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

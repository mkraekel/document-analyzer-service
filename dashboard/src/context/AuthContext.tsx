import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'
import { setToken, clearToken, hasToken } from '../api/client'
import type { LoginResponse } from '../types/api'

interface AuthState {
  isAuthenticated: boolean
  user: string | null
  login: (username: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthState | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(hasToken())
  const [user, setUser] = useState<string | null>(
    localStorage.getItem('auth_user'),
  )

  const login = useCallback(async (username: string, password: string) => {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    })
    if (!res.ok) {
      const data = await res.json().catch(() => ({}))
      throw new Error(data.detail || 'Login fehlgeschlagen')
    }
    const data: LoginResponse = await res.json()
    setToken(data.access_token)
    localStorage.setItem('auth_user', data.user)
    setUser(data.user)
    setIsAuthenticated(true)
  }, [])

  const logout = useCallback(() => {
    clearToken()
    localStorage.removeItem('auth_user')
    setUser(null)
    setIsAuthenticated(false)
  }, [])

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}

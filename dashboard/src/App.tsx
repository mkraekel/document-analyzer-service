import { Routes, Route, Navigate } from 'react-router-dom'
import { Layout } from './components/Layout'
import { ProtectedRoute } from './components/ProtectedRoute'
import { Login } from './pages/Login'
import { Overview } from './pages/Overview'
import { Triage } from './pages/Triage'
import { Cases } from './pages/Cases'
import { CaseDetail } from './pages/CaseDetail'
import { OutgoingEmails } from './pages/OutgoingEmails'

export function App() {
  return (
    <Routes>
      <Route path="/app/login" element={<Login />} />

      <Route
        path="/app"
        element={
          <ProtectedRoute>
            <Layout />
          </ProtectedRoute>
        }
      >
        <Route index element={<Overview />} />
        <Route path="triage" element={<Triage />} />
        <Route path="cases" element={<Cases />} />
        <Route path="cases/:caseId" element={<CaseDetail />} />
        <Route path="emails" element={<OutgoingEmails />} />
      </Route>

      <Route path="*" element={<Navigate to="/app" replace />} />
    </Routes>
  )
}

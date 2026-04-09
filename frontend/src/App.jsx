import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AuthProvider, useAuth } from './contexts/AuthContext'
import AppLayout from './layouts/AppLayout'
import LoginPage from './pages/LoginPage'
import DashboardPage from './pages/DashboardPage'
import HoldingsPage from './pages/HoldingsPage'
import AgentPage from './pages/AgentPage'
import FundDetailPage from './pages/FundDetailPage'
import CompanyDetailPage from './pages/CompanyDetailPage'
import GeographyPage from './pages/GeographyPage'
import GICSPage from './pages/GICSPage'
import ReviewQueuePage from './pages/ops/ReviewQueuePage'
import AuditTrailPage from './pages/ops/AuditTrailPage'
import PipelineMonitorPage from './pages/ops/PipelineMonitorPage'
import FundsPage from './pages/FundsPage'
import SettingsPage from './pages/SettingsPage'
import NotFound from './pages/NotFound'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      retry: 1,
    },
  },
})

function ProtectedRoute({ children }) {
  const { isAuthenticated, isLoading } = useAuth()

  if (isLoading) {
    return (
      <div className="min-h-screen bg-secondary-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin h-8 w-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto"></div>
          <p className="mt-4 text-secondary-500">Loading...</p>
        </div>
      </div>
    )
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return children
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/dashboard" replace />} />
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          <ProtectedRoute>
            <AppLayout />
          </ProtectedRoute>
        }
      >
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/holdings" element={<HoldingsPage />} />
        <Route path="/geography" element={<GeographyPage />} />
        <Route path="/gics" element={<GICSPage />} />
        <Route path="/funds" element={<FundsPage />} />
        <Route path="/funds/:fund_id" element={<FundDetailPage />} />
        <Route path="/companies/:company_id" element={<CompanyDetailPage />} />
        <Route path="/agent" element={<AgentPage />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/ops/review-queue" element={<ReviewQueuePage />} />
        <Route path="/ops/audit-trail" element={<AuditTrailPage />} />
        <Route path="/ops/pipeline" element={<PipelineMonitorPage />} />
      </Route>
      <Route path="*" element={<NotFound />} />
    </Routes>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <AppRoutes />
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

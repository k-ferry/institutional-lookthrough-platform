import { NavLink, Outlet } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import { Button } from '../components/ui/button'
import {
  LayoutDashboard,
  Briefcase,
  Building2,
  Bot,
  Settings,
  LogOut,
  ClipboardList,
  ScrollText,
  Activity,
} from 'lucide-react'
import { cn } from '../lib/utils'

const frontOfficeItems = [
  { to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/holdings', label: 'Holdings', icon: Briefcase },
  { to: '/funds', label: 'Funds', icon: Building2 },
  { to: '/agent', label: 'AI Assistant', icon: Bot },
]

const opsItems = [
  { to: '/ops/review-queue', label: 'Review Queue', icon: ClipboardList },
  { to: '/ops/audit-trail', label: 'Audit Trail', icon: ScrollText },
  { to: '/ops/pipeline', label: 'Pipeline Monitor', icon: Activity },
]

function NavSection({ items }) {
  return items.map(({ to, label, icon: Icon }) => (
    <NavLink
      key={to}
      to={to}
      className={({ isActive }) =>
        cn(
          'flex items-center gap-3 px-4 py-2.5 rounded-md text-sm font-medium transition-colors',
          isActive
            ? 'bg-primary-700 text-white'
            : 'text-primary-100 hover:bg-primary-500 hover:text-white'
        )
      }
    >
      <Icon className="h-5 w-5 shrink-0" />
      {label}
    </NavLink>
  ))
}

export default function AppLayout() {
  const { user, logout } = useAuth()

  return (
    <div className="min-h-screen bg-secondary-50 flex">
      <aside className="w-64 bg-primary-600 text-white flex flex-col">
        <div className="p-6 border-b border-primary-500">
          <h1 className="text-xl font-bold tracking-tight">LookThrough</h1>
          <p className="text-xs text-primary-200 mt-1">Portfolio Transparency</p>
        </div>

        <nav className="flex-1 p-4 space-y-1 overflow-y-auto">
          {/* Front Office section */}
          <p className="px-4 pb-1.5 pt-1 text-[10px] font-semibold uppercase tracking-widest text-primary-300">
            Front Office
          </p>
          <NavSection items={frontOfficeItems} />

          {/* Divider */}
          <div className="my-3 border-t border-primary-500" />

          {/* Ops section */}
          <p className="px-4 pb-1.5 pt-1 text-[10px] font-semibold uppercase tracking-widest text-primary-300">
            Ops
          </p>
          <NavSection items={opsItems} />
        </nav>

        <div className="p-4 border-t border-primary-500">
          <p className="text-xs text-primary-200 mb-2">Version 0.1.0</p>
        </div>
      </aside>

      <div className="flex-1 flex flex-col min-w-0">
        <header className="h-16 bg-white border-b border-secondary-200 flex items-center justify-between px-6 shrink-0">
          <div>
            <h2 className="text-sm font-medium text-secondary-600">
              Institutional Analytics
            </h2>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-sm text-secondary-600">
              {user?.email}
            </span>
            <Button
              variant="ghost"
              size="sm"
              onClick={logout}
              className="text-secondary-600 hover:text-secondary-900"
            >
              <LogOut className="h-4 w-4 mr-2" />
              Logout
            </Button>
          </div>
        </header>

        <main className="flex-1 p-6 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

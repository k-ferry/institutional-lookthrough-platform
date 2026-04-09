import { Settings } from 'lucide-react'

export default function SettingsPage() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] text-center space-y-4">
      <div className="h-16 w-16 rounded-full bg-secondary-100 flex items-center justify-center">
        <Settings className="h-8 w-8 text-secondary-400" />
      </div>
      <h1 className="text-2xl font-bold text-secondary-900">Settings</h1>
      <p className="text-secondary-500 text-sm">Coming Soon</p>
    </div>
  )
}

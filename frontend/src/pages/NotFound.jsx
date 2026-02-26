import { Link } from 'react-router-dom'
import { Button } from '../components/ui/button'
import { Home } from 'lucide-react'

export default function NotFound() {
  return (
    <div className="min-h-screen bg-secondary-50 flex items-center justify-center p-4">
      <div className="text-center">
        <h1 className="text-6xl font-bold text-primary-600">404</h1>
        <p className="mt-4 text-xl text-secondary-600">Page not found</p>
        <p className="mt-2 text-secondary-500">
          The page you're looking for doesn't exist or has been moved.
        </p>
        <Link to="/dashboard" className="inline-block mt-6">
          <Button>
            <Home className="h-4 w-4 mr-2" />
            Back to Dashboard
          </Button>
        </Link>
      </div>
    </div>
  )
}

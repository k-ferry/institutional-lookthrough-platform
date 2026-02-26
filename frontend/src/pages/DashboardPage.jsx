import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card'
import {
  DollarSign,
  Briefcase,
  Building2,
  PieChart,
} from 'lucide-react'

const stats = [
  {
    title: 'Total AUM',
    value: '$2.4B',
    icon: DollarSign,
    description: 'Assets under management',
  },
  {
    title: 'Holdings',
    value: '6,054',
    icon: Briefcase,
    description: 'Individual positions',
  },
  {
    title: 'Companies',
    value: '1,804',
    icon: Building2,
    description: 'Unique entities',
  },
  {
    title: 'Funds',
    value: '11',
    icon: PieChart,
    description: 'Portfolio funds',
  },
]

export default function DashboardPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-secondary-900">Portfolio Overview</h1>
        <p className="text-secondary-500 mt-1">
          Aggregated view across all institutional holdings
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map(({ title, value, icon: Icon, description }) => (
          <Card key={title}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-secondary-500">{title}</p>
                  <p className="text-2xl font-bold text-secondary-900 mt-1">{value}</p>
                  <p className="text-xs text-secondary-400 mt-1">{description}</p>
                </div>
                <div className="h-12 w-12 rounded-full bg-primary-50 flex items-center justify-center">
                  <Icon className="h-6 w-6 text-primary-600" />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Sector Allocation</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64 flex items-center justify-center bg-secondary-50 rounded-lg border-2 border-dashed border-secondary-200">
              <div className="text-center">
                <PieChart className="h-12 w-12 text-secondary-300 mx-auto mb-2" />
                <p className="text-sm text-secondary-500">
                  Sector breakdown chart coming soon
                </p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top Holdings</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64 flex items-center justify-center bg-secondary-50 rounded-lg border-2 border-dashed border-secondary-200">
              <div className="text-center">
                <Briefcase className="h-12 w-12 text-secondary-300 mx-auto mb-2" />
                <p className="text-sm text-secondary-500">
                  Holdings table coming soon
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

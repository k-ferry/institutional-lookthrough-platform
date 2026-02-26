import { authClient } from './client'

export async function loginUser(email, password) {
  const response = await authClient.post('/login', { email, password })
  return response.data
}

export async function logoutUser() {
  const response = await authClient.post('/logout')
  return response.data
}

export async function getCurrentUser() {
  const response = await authClient.get('/me')
  return response.data
}

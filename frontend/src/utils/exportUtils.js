import axios from 'axios'

/**
 * Trigger a browser file download from a Blob.
 * Creates a temporary object URL, clicks a hidden anchor, then cleans up.
 */
export function exportToFile(blob, filename) {
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

/**
 * Fetch a file from a protected API endpoint and trigger a browser download.
 * Uses axios with responseType: 'blob' and withCredentials for cookie auth.
 */
export async function downloadExport(url, filename) {
  const response = await axios.get(url, {
    responseType: 'blob',
    withCredentials: true,
  })
  exportToFile(response.data, filename)
}

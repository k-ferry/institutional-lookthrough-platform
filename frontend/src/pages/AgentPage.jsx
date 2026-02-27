import { useState, useRef, useEffect } from 'react'
import { Send, Bot, User, AlertCircle, Sparkles } from 'lucide-react'
import apiClient from '../api/client'

const STARTER_QUESTIONS = [
  'What is my total exposure to healthcare?',
  'Which fund has the highest concentration in financials?',
  'Show me my top 10 holdings by value',
  'What percentage of my portfolio is in private credit?',
]

// Render agent text: handle **bold** and newlines
function FormattedText({ text }) {
  return (
    <>
      {text.split('\n').map((line, lineIdx, lines) => {
        const parts = line.split(/(\*\*[^*]+\*\*)/)
        return (
          <span key={lineIdx}>
            {parts.map((part, partIdx) =>
              part.startsWith('**') && part.endsWith('**') ? (
                <strong key={partIdx}>{part.slice(2, -2)}</strong>
              ) : (
                part
              )
            )}
            {lineIdx < lines.length - 1 && <br />}
          </span>
        )
      })}
    </>
  )
}

function MessageBubble({ message }) {
  const isUser = message.role === 'user'
  const time = message.timestamp.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
  })

  if (isUser) {
    return (
      <div className="flex justify-end mb-5">
        <div className="flex flex-col items-end gap-1 max-w-[70%]">
          <div className="bg-primary-600 text-white rounded-2xl rounded-tr-sm px-4 py-3 text-sm leading-relaxed shadow-sm">
            {message.content}
          </div>
          <span className="text-xs text-secondary-400 pr-1">{time}</span>
        </div>
        <div className="ml-3 mt-0.5 flex-shrink-0 w-8 h-8 rounded-full bg-primary-600 flex items-center justify-center">
          <User className="w-4 h-4 text-white" />
        </div>
      </div>
    )
  }

  return (
    <div className="flex justify-start mb-5">
      <div className="mr-3 mt-0.5 flex-shrink-0 w-8 h-8 rounded-full bg-secondary-100 border border-secondary-200 flex items-center justify-center">
        <Bot className="w-4 h-4 text-primary-600" />
      </div>
      <div className="flex flex-col items-start gap-1 max-w-[70%]">
        <div
          className={`bg-white border rounded-2xl rounded-tl-sm px-4 py-3 text-sm leading-relaxed shadow-sm ${
            message.isError
              ? 'border-red-200 text-red-700'
              : 'border-secondary-200 text-secondary-800'
          }`}
        >
          {message.isError && (
            <span className="inline-flex items-center gap-1.5 mb-1 text-red-600">
              <AlertCircle className="w-3.5 h-3.5" />
            </span>
          )}
          <FormattedText text={message.content} />
          {message.sources && message.sources.length > 0 && (
            <div className="mt-2.5 pt-2.5 border-t border-secondary-100 flex flex-wrap gap-1.5">
              {message.sources.map((src, i) => (
                <span
                  key={i}
                  className="inline-block text-xs bg-secondary-50 text-secondary-500 border border-secondary-200 rounded px-2 py-0.5 font-mono"
                >
                  {src.name}
                </span>
              ))}
            </div>
          )}
        </div>
        <span className="text-xs text-secondary-400 pl-1">{time}</span>
      </div>
    </div>
  )
}

function LoadingIndicator() {
  return (
    <div className="flex justify-start mb-5">
      <div className="mr-3 mt-0.5 flex-shrink-0 w-8 h-8 rounded-full bg-secondary-100 border border-secondary-200 flex items-center justify-center">
        <Bot className="w-4 h-4 text-primary-600" />
      </div>
      <div className="bg-white border border-secondary-200 rounded-2xl rounded-tl-sm px-4 py-3 shadow-sm">
        <div className="flex gap-1.5 items-center h-5">
          {[0, 150, 300].map((delay) => (
            <div
              key={delay}
              className="w-2 h-2 bg-secondary-400 rounded-full animate-bounce"
              style={{ animationDelay: `${delay}ms` }}
            />
          ))}
        </div>
      </div>
    </div>
  )
}

function EmptyState({ onSelect }) {
  return (
    <div className="flex flex-col items-center justify-center h-full px-6 text-center">
      <div className="w-14 h-14 rounded-full bg-primary-50 border border-primary-100 flex items-center justify-center mb-4">
        <Sparkles className="w-7 h-7 text-primary-600" />
      </div>
      <h3 className="text-base font-semibold text-secondary-800 mb-1">
        Portfolio AI Assistant
      </h3>
      <p className="text-sm text-secondary-500 mb-8 max-w-sm">
        Ask questions about your portfolio exposures, holdings, and risk
        concentrations. I query live data before answering.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 w-full max-w-xl">
        {STARTER_QUESTIONS.map((q) => (
          <button
            key={q}
            onClick={() => onSelect(q)}
            className="text-left text-sm text-secondary-700 bg-white border border-secondary-200 hover:border-primary-300 hover:bg-primary-50 hover:text-primary-700 rounded-lg px-4 py-3 transition-colors shadow-sm"
          >
            {q}
          </button>
        ))}
      </div>
    </div>
  )
}

export default function AgentPage() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef(null)
  const inputRef = useRef(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isLoading])

  async function sendMessage(text) {
    const trimmed = text.trim()
    if (!trimmed || isLoading) return

    const userMessage = { role: 'user', content: trimmed, timestamp: new Date() }

    // Snapshot history before adding the new user message
    const historySnapshot = messages.map(({ role, content }) => ({ role, content }))

    setMessages((prev) => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      const { data } = await apiClient.post('/agent/chat', {
        message: trimmed,
        conversation_history: historySnapshot,
      })
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: data.response,
          sources: data.sources || [],
          timestamp: new Date(),
        },
      ])
    } catch (err) {
      const detail = err.response?.data?.detail
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content:
            detail ||
            "I'm sorry, I couldn't process your request. Please try again.",
          sources: [],
          timestamp: new Date(),
          isError: true,
        },
      ])
    } finally {
      setIsLoading(false)
      inputRef.current?.focus()
    }
  }

  function handleSubmit(e) {
    e.preventDefault()
    sendMessage(input)
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  return (
    <div
      className="flex flex-col bg-secondary-50 rounded-lg border border-secondary-200 overflow-hidden"
      style={{ height: 'calc(100vh - 140px)' }}
    >
      {/* Header */}
      <div className="flex-shrink-0 px-6 py-3 bg-white border-b border-secondary-200 flex items-center gap-3">
        <div className="w-8 h-8 rounded-full bg-primary-600 flex items-center justify-center">
          <Bot className="w-4 h-4 text-white" />
        </div>
        <div>
          <p className="text-sm font-semibold text-secondary-800">AI Portfolio Assistant</p>
          <p className="text-xs text-secondary-400">Queries live portfolio data · Powered by Claude</p>
        </div>
        {messages.length > 0 && (
          <button
            onClick={() => setMessages([])}
            className="ml-auto text-xs text-secondary-400 hover:text-secondary-600 transition-colors"
          >
            Clear chat
          </button>
        )}
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto px-6 py-5">
        {messages.length === 0 && !isLoading ? (
          <EmptyState onSelect={sendMessage} />
        ) : (
          <>
            {messages.map((msg, i) => (
              <MessageBubble key={i} message={msg} />
            ))}
            {isLoading && <LoadingIndicator />}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {/* Input bar */}
      <div className="flex-shrink-0 bg-white border-t border-secondary-200 px-4 py-3">
        <form onSubmit={handleSubmit} className="flex items-end gap-3">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about your portfolio exposures…"
            rows={1}
            disabled={isLoading}
            className="flex-1 resize-none rounded-lg border border-secondary-300 bg-secondary-50 px-4 py-2.5 text-sm text-secondary-800 placeholder:text-secondary-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent disabled:opacity-50 leading-relaxed"
            style={{ maxHeight: '120px', overflowY: 'auto' }}
            onInput={(e) => {
              e.target.style.height = 'auto'
              e.target.style.height = `${Math.min(e.target.scrollHeight, 120)}px`
            }}
          />
          <button
            type="submit"
            disabled={!input.trim() || isLoading}
            className="flex-shrink-0 w-10 h-10 rounded-lg bg-primary-600 text-white flex items-center justify-center hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            <Send className="w-4 h-4" />
          </button>
        </form>
        <p className="mt-1.5 text-xs text-secondary-400 pl-1">
          Press Enter to send · Shift+Enter for new line
        </p>
      </div>
    </div>
  )
}

import React, { useState } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../auth/AuthProvider'
import { answerDashboardQuestion, type AssistantAnswer } from '../dashboard/assistant'
import { useDashboardData } from '../dashboard/DashboardDataProvider'
import './AppShell.css'

const navItems = [
  { to: '/dashboard', label: 'Dashboard', icon: <DashboardIcon /> },
  { to: '/documents', label: 'Documents Processing', icon: <DocumentIcon /> },
  { to: '/settings', label: 'Settings', icon: <SettingsIcon /> },
]

type ChatMessage = {
  role: 'user' | 'assistant'
  text?: string
  answer?: AssistantAnswer
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const { logout } = useAuth()
  const navigate = useNavigate()
  const dashboard = useDashboardData()
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [chatInput, setChatInput] = useState('')
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([])

  function handleSend() {
    const trimmed = chatInput.trim()
    if (!trimmed) return
    const assistantReply = answerDashboardQuestion(trimmed, {
      snapshotId: dashboard.snapshotId,
      snapshots: dashboard.snapshots,
      kpis: dashboard.kpis,
      clients: dashboard.clients,
      staff: dashboard.staff,
    })

    setChatMessages((current) => [
      ...current,
      { role: 'user', text: trimmed },
      { role: 'assistant', answer: assistantReply },
    ])
    setChatInput('')
  }

  return (
    <div className={`shell${sidebarCollapsed ? ' collapsed' : ''}`}>
      <aside className="shell-sidebar">
        <div className="shell-sidebar-top">
          <div className="shell-brand-wrap">
            <div className="shell-brand-mark">T</div>
            <div className="shell-brand-copy">
              <div className="shell-brand-title">Tax Assistant</div>
              <div className="shell-brand-subtitle">Ask anything about the dashboard</div>
            </div>
          </div>
          <button
            className="shell-collapse-button"
            onClick={() => setSidebarCollapsed((current) => !current)}
            type="button"
            aria-label={sidebarCollapsed ? 'Expand tax assistant' : 'Collapse tax assistant'}
            aria-expanded={!sidebarCollapsed}
          >
            <ChevronIcon collapsed={sidebarCollapsed} />
          </button>
        </div>

        <section className="shell-chat-log" aria-label="Tax assistant conversation">
          {chatMessages.map((message, index) => (
            <div key={`${message.role}-${index}`} className={`shell-chat-message ${message.role}`}>
              {message.role === 'user' ? (
                message.text
              ) : (
                <AssistantMessageCard answer={message.answer as AssistantAnswer} />
              )}
            </div>
          ))}
        </section>

        <div className="shell-sidebar-footer">
          <form
            className="shell-compose"
            onSubmit={(event) => {
              event.preventDefault()
              handleSend()
            }}
          >
            <label className="shell-compose-label" htmlFor="tax-assistant-input">
              Ask anything about this dashboard...
            </label>
            <textarea
              id="tax-assistant-input"
              className="shell-compose-input"
              rows={3}
              value={chatInput}
              onChange={(event) => setChatInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                  event.preventDefault()
                  handleSend()
                }
              }}
              placeholder="Type your question here"
            />
            <div className="shell-compose-actions">
              <button className="shell-compose-secondary" type="button" aria-label="Add">
                +
              </button>
              <button className="shell-compose-secondary" type="button" aria-label="Microphone">
                <MicIcon />
              </button>
              <button className="shell-compose-send" type="submit" aria-label="Send" disabled={!chatInput.trim()}>
                <SendIcon />
              </button>
            </div>
          </form>

          <button
            className="shell-logout"
            onClick={() => {
              logout()
              navigate('/login')
            }}
            type="button"
          >
            Logout
          </button>
        </div>
      </aside>

      <div className="shell-main">
        <header className="shell-topbar">
          <nav className="shell-nav">
            {navItems.map((item) => (
              <NavLink key={item.to} to={item.to} className={({ isActive }) => `shell-nav-link${isActive ? ' active' : ''}`}>
                {item.icon}
                <span>{item.label}</span>
              </NavLink>
            ))}
          </nav>

          <div className="shell-topbar-right">
            <button className="shell-icon-button" type="button" aria-label="Notifications">
              <BellIcon />
            </button>
            <div className="shell-avatar">YK</div>
            <button className="shell-icon-button" type="button" aria-label="Menu">
              <MenuIcon />
            </button>
            <div className="shell-wordmark">logoipsum</div>
          </div>
        </header>

        <main className="shell-content">{children}</main>
      </div>
    </div>
  )
}

function AssistantMessageCard({ answer }: { answer: AssistantAnswer }) {
  return (
    <div className="shell-answer-card">
      <div className="shell-answer-title">{answer.title}</div>
      {answer.summary && <div className="shell-answer-summary">{answer.summary}</div>}
      {answer.cards && answer.cards.length > 0 && (
        <div className="shell-answer-cards">
          {answer.cards.map((card) => (
            <div key={`${card.title}-${card.value}`} className="shell-answer-metric">
              <div className="shell-answer-metric-title">{card.title}</div>
              <div className="shell-answer-metric-value">{card.value}</div>
              {card.meta && <div className="shell-answer-metric-meta">{card.meta}</div>}
            </div>
          ))}
        </div>
      )}
      {answer.bullets && answer.bullets.length > 0 && (
        <div className="shell-answer-bullets">
          {answer.bullets.map((bullet) => (
            <div key={bullet} className="shell-answer-bullet">
              {bullet}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function DashboardIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 13h7V4H4v9Zm0 7h7v-5H4v5Zm9 0h7V11h-7v9Zm0-18v7h7V2h-7Z" fill="currentColor" />
    </svg>
  )
}

function DocumentIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M7 3.75A2.25 2.25 0 0 0 4.75 6v12A2.25 2.25 0 0 0 7 20.25h10A2.25 2.25 0 0 0 19.25 18V9.56a2.25 2.25 0 0 0-.66-1.59l-3.56-3.56a2.25 2.25 0 0 0-1.59-.66H7Zm6 1.7 4.55 4.55H13V5.45Zm-4 6.05h6.5a.75.75 0 0 1 0 1.5H9a.75.75 0 0 1 0-1.5Zm0 3.5h6.5a.75.75 0 0 1 0 1.5H9A.75.75 0 0 1 9 15Z"
        fill="currentColor"
      />
    </svg>
  )
}

function SettingsIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="m12 2 1.19 2.75 2.98.35-2.25 2 .66 2.9L12 8.45 9.42 10l.66-2.9-2.25-2 2.98-.35L12 2Zm7.5 10 1.5 1-1.5 1 .27 1.76-1.74.52-.74 1.63-1.73-.57-1.42 1.1-1.42-1.1-1.73.57-.74-1.63-1.74-.52L4.5 14l-1.5-1 1.5-1-.27-1.76 1.74-.52.74-1.63 1.73.57 1.42-1.1 1.42 1.1 1.73-.57.74 1.63 1.74.52L19.5 12Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <circle cx="12" cy="13" r="2.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
    </svg>
  )
}

function BellIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M12 4a4 4 0 0 0-4 4v2.4c0 .52-.18 1.02-.52 1.41L6 13.5V15h12v-1.5l-1.48-1.69a2.13 2.13 0 0 1-.52-1.41V8a4 4 0 0 0-4-4Zm-1.5 13h3a1.5 1.5 0 0 1-3 0Z"
        fill="currentColor"
      />
    </svg>
  )
}

function MenuIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 7.25h14M5 12h14M5 16.75h14" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  )
}

function MicIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M12 15.25A3.25 3.25 0 0 0 15.25 12V7a3.25 3.25 0 1 0-6.5 0v5A3.25 3.25 0 0 0 12 15.25ZM6.75 11.75a.75.75 0 0 1 1.5 0 3.75 3.75 0 1 0 7.5 0 .75.75 0 0 1 1.5 0A5.26 5.26 0 0 1 12.75 17v2h2a.75.75 0 0 1 0 1.5h-5.5a.75.75 0 0 1 0-1.5h2v-2a5.26 5.26 0 0 1-4.5-5.25Z"
        fill="currentColor"
      />
    </svg>
  )
}

function SendIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M19.68 4.34a1 1 0 0 0-1.06-.2L4.76 9.62a1 1 0 0 0 .06 1.88l5.35 1.71 1.71 5.35a1 1 0 0 0 1.88.06l5.48-13.86a1 1 0 0 0-.56-1.42ZM11.15 12.85l-4.18-1.33 8.8-3.48-4.62 4.81Zm1.33 4.18-1.33-4.18 4.81-4.62-3.48 8.8Z"
        fill="currentColor"
      />
    </svg>
  )
}

function ChevronIcon({ collapsed }: { collapsed: boolean }) {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path
        d={collapsed ? 'm9 5 6 7-6 7' : 'm15 5-6 7 6 7'}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

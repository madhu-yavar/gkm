import React, { useState } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useApi } from '../api'
import { useAuth } from '../auth/AuthProvider'
import type { AssistantAnswer } from '../dashboard/assistant'
import { useDashboardData } from '../dashboard/DashboardDataProvider'
import { LayoutDashboard, FileText, Settings, Bell, Menu, Mic, Send, ChevronLeft, ChevronRight, LogOut, Trash2 } from 'lucide-react'

type ChatMessage = { role: 'user' | 'assistant'; text?: string; answer?: AssistantAnswer }

export function AppShell({ children }: { children: React.ReactNode }) {
  const api = useApi()
  const { logout } = useAuth()
  const navigate = useNavigate()
  const dashboard = useDashboardData()
  const [collapsed, setCollapsed] = useState(false)
  const [chatInput, setChatInput] = useState('')
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([])
  const [chatLoading, setChatLoading] = useState(false)

  function clearChat() {
    setChatMessages([])
    setChatInput('')
  }

  async function handleSend() {
    const trimmed = chatInput.trim()
    if (!trimmed) return
    setChatMessages((c) => [...c, { role: 'user', text: trimmed }])
    setChatInput('')
    setChatLoading(true)
    try {
      const reply = await api.askDashboardQuestion(trimmed, dashboard.snapshotId)
      setChatMessages((c) => [...c, { role: 'assistant', answer: reply }])
    } catch (error) {
      const message = error instanceof Error && error.message.trim()
        ? error.message
        : 'Agentic chat failed for this snapshot.'
      setChatMessages((c) => [...c, {
        role: 'assistant',
        answer: {
          title: 'Chat Unavailable',
          summary: message,
          bullets: ['Enable Gemini in Settings or fix the active model configuration before retrying.'],
        },
      }])
    } finally {
      setChatLoading(false)
    }
  }

  return (
    <div className={`grid min-h-screen ${collapsed ? 'grid-cols-[84px_1fr]' : 'grid-cols-[320px_1fr]'} transition-all duration-300 max-lg:grid-cols-1`}>
      {/* Sidebar */}
      <aside className={`flex flex-col gap-3 p-4 border-r border-border bg-sidebar sticky top-0 h-screen overflow-hidden max-lg:hidden`}
        style={{ background: 'linear-gradient(180deg, hsla(262, 78%, 38%, 0.18) 0%, hsla(266, 66%, 30%, 0.10) 22%, hsl(var(--sidebar-background)) 54%)' }}>
        
        {/* Brand */}
        <div className="flex items-center justify-between gap-2 pb-3 border-b border-border">
          <div className="flex items-center gap-2.5 min-w-0">
            <div className="grid place-items-center w-9 h-9 rounded-xl border border-primary/20 text-primary-foreground font-extrabold text-sm shrink-0 shadow-soft"
              style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}>Z</div>
            {!collapsed && (
              <div className="min-w-0">
                <div className="text-sm font-bold text-foreground leading-tight truncate">Dashboard Assistant</div>
                <div className="text-[0.7rem] text-muted-foreground leading-tight">Ask anything about the dashboard</div>
              </div>
            )}
          </div>
          <button
            onClick={() => setCollapsed((c) => !c)}
            className="grid place-items-center w-7 h-7 rounded-lg border border-primary/15 bg-primary/5 text-primary hover:bg-primary/10 transition shrink-0"
          >
            {collapsed ? <ChevronRight className="w-3.5 h-3.5" /> : <ChevronLeft className="w-3.5 h-3.5" />}
          </button>
        </div>

        {/* Chat Log */}
        {!collapsed && (
          <div className="flex flex-col gap-2 flex-1 min-h-0 overflow-auto rounded-[1.4rem] border border-primary/10 bg-[linear-gradient(180deg,hsla(262,83%,58%,0.05),hsla(270,70%,65%,0.02))] p-2.5 pr-2">
            <div className="flex items-center justify-end">
              <button
                type="button"
                onClick={clearChat}
                disabled={(chatMessages.length === 0 && !chatInput.trim()) || chatLoading}
                className="inline-flex items-center gap-1.5 rounded-lg border border-primary/15 bg-primary/5 px-2.5 py-1.5 text-[0.7rem] font-semibold text-primary transition hover:bg-primary/10 disabled:cursor-not-allowed disabled:opacity-40"
              >
                <Trash2 className="h-3.5 w-3.5" />
                Clear Chat
              </button>
            </div>
            {chatMessages.map((msg, i) => (
              <div key={i} className={`max-w-full rounded-xl text-[0.78rem] leading-relaxed ${
                msg.role === 'user'
                  ? 'self-end border border-primary/20 px-3 py-2 text-primary-foreground shadow-soft'
                  : ''
              }`}
                style={msg.role === 'user'
                  ? { background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }
                  : undefined}>
                {msg.role === 'user' ? msg.text : <AnswerCard answer={msg.answer!} />}
              </div>
            ))}
            {chatLoading && (
              <div className="max-w-full rounded-xl">
                <AnswerCard answer={{ title: 'Thinking', summary: 'Reviewing the active dashboard context.' }} />
              </div>
            )}
          </div>
        )}

        {/* Compose */}
        {!collapsed && (
          <div className="flex flex-col gap-2.5 shrink-0 mt-auto">
            <form onSubmit={(e) => { e.preventDefault(); void handleSend() }}
              className="rounded-2xl border border-primary/12 p-3 shadow-soft"
              style={{ background: 'linear-gradient(180deg, hsla(262, 90%, 97%, 0.94), hsla(268, 70%, 94%, 0.92))', backdropFilter: 'blur(12px)' }}>
              <label className="block text-primary text-[0.76rem] font-semibold mb-2">Ask anything about this dashboard...</label>
              <textarea rows={3} value={chatInput} onChange={(e) => setChatInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); void handleSend() } }}
                placeholder="Type your question here"
                className="w-full resize-none rounded-lg border border-primary/12 bg-white/80 px-3 py-2.5 text-foreground text-[0.8rem] leading-relaxed placeholder:text-muted-foreground/60 focus:outline-none focus:border-primary/40 transition" />
              <div className="flex items-center gap-2.5 mt-2">
                <button type="button" className="grid place-items-center w-7 h-7 rounded-full bg-primary/10 text-primary hover:bg-primary/15 transition">+</button>
                <button type="button" className="grid place-items-center w-7 h-7 rounded-full bg-primary/10 text-primary hover:bg-primary/15 transition"><Mic className="w-3.5 h-3.5" /></button>
                <button
                  type="submit"
                  disabled={!chatInput.trim() || chatLoading}
                  className="ml-auto grid place-items-center w-7 h-7 rounded-full text-primary-foreground disabled:opacity-40 transition hover:opacity-90"
                  style={{ background: 'linear-gradient(135deg, hsl(var(--primary)), hsl(var(--brand-glow)))' }}
                >
                  <Send className="w-3.5 h-3.5" />
                </button>
              </div>
            </form>

            <button onClick={() => { logout(); navigate('/login') }}
              className="w-full py-3 px-4 rounded-xl border border-border bg-card text-foreground font-semibold text-sm hover:bg-accent transition flex items-center justify-center gap-2">
              <LogOut className="w-4 h-4" /> Logout
            </button>
          </div>
        )}
      </aside>

      {/* Main Area */}
      <div className="min-w-0 flex flex-col">
        {/* Top Bar */}
        <header className="flex items-center justify-between gap-4 px-5 py-3 border-b border-border bg-card/90 backdrop-blur-xl sticky top-0 z-10">
          <nav className="flex items-center gap-1.5 flex-wrap">
            {[
              { to: '/dashboard', label: 'Dashboard', icon: <LayoutDashboard className="w-4 h-4" /> },
              { to: '/documents', label: 'Documents Processing', icon: <FileText className="w-4 h-4" /> },
              { to: '/settings', label: 'Settings', icon: <Settings className="w-4 h-4" /> },
            ].map((item) => (
              <NavLink key={item.to} to={item.to}
                className={({ isActive }) =>
                  `inline-flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm font-semibold transition-all duration-200 ${
                    isActive
                      ? 'bg-primary/10 text-primary shadow-[inset_0_-2px_0_hsl(var(--primary)/0.4)]'
                      : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                  }`
                }>
                {item.icon}
                <span>{item.label}</span>
              </NavLink>
            ))}
          </nav>

          <div className="flex items-center gap-2.5">
            <button className="grid place-items-center w-9 h-9 rounded-lg bg-card text-muted-foreground border border-border hover:text-foreground transition"><Bell className="w-4 h-4" /></button>
            <div className="grid place-items-center w-9 h-9 rounded-full text-[0.7rem] font-extrabold text-primary-foreground" style={{ background: 'linear-gradient(135deg, #f6c2aa, hsl(var(--primary)))' }}>YK</div>
            <button className="grid place-items-center w-9 h-9 rounded-lg bg-card text-muted-foreground border border-border hover:text-foreground transition"><Menu className="w-4 h-4" /></button>
            <div className="pl-3 ml-0.5 border-l border-border">
              <img src="/logo.png" alt="yavar" className="h-8 w-auto object-contain" />
            </div>
          </div>
        </header>

        <main className="p-5 flex-1">{children}</main>
      </div>
    </div>
  )
}

function AnswerCard({ answer }: { answer: AssistantAnswer }) {
  return (
    <div
      className="rounded-2xl border border-primary/10 p-3 animate-fade-in shadow-soft"
      style={{ background: 'linear-gradient(180deg, hsla(262, 90%, 99%, 0.98), hsla(268, 55%, 95%, 0.96))' }}
    >
      <div className="text-[0.8rem] font-bold text-foreground">{answer.title}</div>
      {answer.summary && <div className="mt-1 text-muted-foreground text-[0.76rem] leading-relaxed">{answer.summary}</div>}
      {answer.cards && answer.cards.length > 0 && (
        <div className="grid gap-2 mt-2.5">
          {answer.cards.map((card) => (
            <div
              key={`${card.title}-${card.value}`}
              className="rounded-xl border border-primary/10 p-2.5"
              style={{ background: 'linear-gradient(180deg, hsla(262, 100%, 98%, 0.95), hsla(268, 70%, 96%, 0.92))' }}
            >
              <div className="text-[0.7rem] text-primary">{card.title}</div>
              <div className="text-sm font-bold text-foreground">{card.value}</div>
              {card.meta && <div className="mt-0.5 text-[0.72rem] text-muted-foreground">{card.meta}</div>}
            </div>
          ))}
        </div>
      )}
      {answer.bullets && answer.bullets.length > 0 && (
        <div className="flex flex-col gap-1.5 mt-2.5">
          {answer.bullets.map((b) => (
            <div key={b} className="pl-3 relative text-muted-foreground text-[0.76rem] leading-relaxed before:content-[''] before:absolute before:left-0 before:top-[0.44rem] before:w-1.5 before:h-1.5 before:rounded-full before:bg-primary/45">{b}</div>
          ))}
        </div>
      )}
    </div>
  )
}

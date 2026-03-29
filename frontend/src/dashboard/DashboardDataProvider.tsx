import React, { createContext, useContext, useEffect, useState } from 'react'
import { useApi, type DashboardBlueprintResponse, type DashboardRuntimeResponse } from '../api'

export type Snapshot = { id: number; as_of_date: string; source_filename: string }

export type Kpis = {
  snapshot: Snapshot
  total_contracted: number
  total_received: number
  total_pending: number
  total_contracted_ind: number
  total_contracted_bus: number
  total_received_ind: number
  total_received_bus: number
  overall_receipt_rate: number
  active_clients: number
  zero_received_clients: number
  over_delivered_clients: number
  staff_total_received: number
}

export type ClientRow = {
  client_name: string
  client_id: string
  client_type: string
  contracted_ind: number
  contracted_bus: number
  contracted_total: number
  received_ind: number
  received_bus: number
  received_total: number
  pending_ind: number
  pending_bus: number
  pending_total: number
  receipt_rate: number | null
}

export type StaffRow = {
  name: string
  staff_id: string
  staff_type: string
  received_ind: number
  received_bus: number
  received_total: number
}

type DashboardDataContextValue = {
  snapshots: Snapshot[]
  snapshotId: number | undefined
  setSnapshotId: React.Dispatch<React.SetStateAction<number | undefined>>
  kpis: Kpis | null
  clients: ClientRow[]
  staff: StaffRow[]
  blueprint: DashboardBlueprintResponse | null
  runtime: DashboardRuntimeResponse | null
  loading: boolean
  error: string | null
  refreshSnapshots: () => Promise<void>
  refreshDashboard: (nextSnapshotId?: number | undefined) => Promise<void>
  refreshBlueprint: (nextSnapshotId?: number | undefined) => Promise<void>
}

const DashboardDataContext = createContext<DashboardDataContextValue | undefined>(undefined)

export function DashboardDataProvider({ children }: { children: React.ReactNode }) {
  const api = useApi()
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [snapshotId, setSnapshotId] = useState<number | undefined>(undefined)
  const [kpis, setKpis] = useState<Kpis | null>(null)
  const [clients, setClients] = useState<ClientRow[]>([])
  const [staff, setStaff] = useState<StaffRow[]>([])
  const [blueprint, setBlueprint] = useState<DashboardBlueprintResponse | null>(null)
  const [runtime, setRuntime] = useState<DashboardRuntimeResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function refreshSnapshots() {
    try {
      const rows = await api.listSnapshots()
      setSnapshots(rows)
      setSnapshotId((current) => {
        if (current && rows.some((row) => row.id === current)) return current
        return rows[0]?.id
      })
    } catch {
      setSnapshots([])
      setSnapshotId(undefined)
      setKpis(null)
      setClients([])
      setStaff([])
    }
  }

  async function refreshDashboard(nextSnapshotId?: number | undefined) {
    const effectiveSnapshotId = nextSnapshotId ?? snapshotId
    if (!effectiveSnapshotId) {
      setKpis(null)
      setClients([])
      setStaff([])
      setBlueprint(null)
      setRuntime(null)
      return
    }

    if (nextSnapshotId !== undefined && nextSnapshotId !== snapshotId) {
      setSnapshotId(nextSnapshotId)
    }

    setLoading(true)
    setError(null)
    try {
      const [nextKpis, nextClients, nextStaff] = await Promise.all([
        api.getKpis(effectiveSnapshotId),
        api.listClients(effectiveSnapshotId),
        api.listStaff(effectiveSnapshotId),
      ])
      let nextBlueprint: DashboardBlueprintResponse | null = null
      let nextRuntime: DashboardRuntimeResponse | null = null
      try {
        nextBlueprint = await api.getDashboardBlueprint(effectiveSnapshotId)
      } catch {
        nextBlueprint = null
      }
      try {
        nextRuntime = await api.getDashboardRuntime(effectiveSnapshotId)
      } catch {
        nextRuntime = null
      }
      setKpis(nextKpis)
      setClients(nextClients)
      setStaff(nextStaff)
      setBlueprint(nextBlueprint)
      setRuntime(nextRuntime)
    } catch {
      setError('Failed to load dashboard. Upload an Excel snapshot first.')
    } finally {
      setLoading(false)
    }
  }

  async function refreshBlueprint(nextSnapshotId?: number | undefined) {
    const effectiveSnapshotId = nextSnapshotId ?? snapshotId
    if (!effectiveSnapshotId) {
      setBlueprint(null)
      setRuntime(null)
      return
    }
    if (nextSnapshotId !== undefined && nextSnapshotId !== snapshotId) {
      setSnapshotId(nextSnapshotId)
    }
    const nextBlueprint = await api.getDashboardBlueprint(effectiveSnapshotId)
    setBlueprint(nextBlueprint)
  }

  useEffect(() => {
    void refreshSnapshots()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    void refreshDashboard(snapshotId)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snapshotId])

  return (
    <DashboardDataContext.Provider
      value={{
        snapshots,
        snapshotId,
        setSnapshotId,
        kpis,
        clients,
        staff,
        blueprint,
        runtime,
        loading,
        error,
        refreshSnapshots,
        refreshDashboard,
        refreshBlueprint,
      }}
    >
      {children}
    </DashboardDataContext.Provider>
  )
}

export function useDashboardData() {
  const context = useContext(DashboardDataContext)
  if (!context) throw new Error('useDashboardData must be used within DashboardDataProvider')
  return context
}

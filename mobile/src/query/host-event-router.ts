import { useCallback, useEffect, useSyncExternalStore } from 'react'
import { focusManager, useQueryClient, type Query, type QueryClient } from '@tanstack/react-query'
import type { RpcClient } from '../transport/rpc-client'
import {
  subscribeSharedClientEvents,
  type SharedClientEvent
} from '../transport/shared-client-events'
import type { HostQuerySignal } from './host-query'
import { hostScopeKey } from './host-query-key'

function signalOf(query: Query): HostQuerySignal | undefined {
  // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: hostQueryOptions writes `signal` from the family's HostQuerySignal.
  return query.meta?.signal as HostQuerySignal | undefined
}

type ChangeEvent = Exclude<SharedClientEvent, { type: 'ready' | 'end' | 'error' }>

function signalMatches(signal: HostQuerySignal, event: ChangeEvent): boolean {
  if (!signal.events.includes(event.type)) {
    return false
  }
  if (event.type !== 'settingsChanged' || !signal.settingsKeys || !Array.isArray(event.keys)) {
    return true
  }
  return event.keys.some((key) => signal.settingsKeys?.includes(key))
}

// What to do once a read already out settles: read again, or only mark the answer stale.
type AfterRead = 'refetch' | 'stale'

const afterReads = new WeakMap<QueryClient, Map<string, AfterRead>>()

function afterReadsOf(queryClient: QueryClient): Map<string, AfterRead> {
  const existing = afterReads.get(queryClient)
  if (existing) {
    return existing
  }
  const pending = new Map<string, AfterRead>()
  afterReads.set(queryClient, pending)
  // One cache listener per world, for as long as the world's client lives.
  queryClient.getQueryCache().subscribe(({ type, query }) => {
    const after = pending.get(query.queryHash)
    if (type === 'updated' && after && query.state.fetchStatus === 'idle') {
      pending.delete(query.queryHash)
      if (after === 'refetch') {
        refetchHostQueryAfterInFlight(queryClient, query)
      } else {
        void markStale(queryClient, query)
      }
    }
  })
  return pending
}

function markStale(queryClient: QueryClient, query: Query): Promise<void> {
  return queryClient.invalidateQueries({
    queryKey: query.queryKey,
    exact: true,
    refetchType: 'none'
  })
}

// A read already out may predate the change, and its answer would clear the mark; mark again once it lands.
function markStaleAfterInFlight(queryClient: QueryClient, query: Query): void {
  void markStale(queryClient, query)
  const pending = afterReadsOf(queryClient)
  if (query.state.fetchStatus === 'fetching' && !pending.has(query.queryHash)) {
    pending.set(query.queryHash, 'stale')
  }
}

/**
 * Reads an observed query again, after the read already out settles if there is one: that read may
 * have been sent before the change that asked. TanStack's own refetch would join it or cancel it.
 */
export function refetchHostQueryAfterInFlight(queryClient: QueryClient, query: Query): void {
  if (query.state.fetchStatus === 'fetching') {
    afterReadsOf(queryClient).set(query.queryHash, 'refetch')
    return
  }
  void queryClient.refetchQueries(
    { queryKey: query.queryKey, exact: true, type: 'active' },
    { cancelRefetch: false }
  )
}

type RouterHandle = {
  stop: () => void
  isReady: () => boolean
  onReadyChange: (listener: () => void) => () => void
}

function startHostEventRouter(
  queryClient: QueryClient,
  hostId: string,
  client: RpcClient
): RouterHandle {
  let ready = false
  const readyListeners = new Set<() => void>()
  const setReady = (next: boolean): void => {
    if (next !== ready) {
      ready = next
      readyListeners.forEach((listener) => listener())
    }
  }
  const startedAt = Date.now()
  // When the last stream ended; the next one's ready follows changes nobody heard.
  let endedAt: number | null = null
  let stopStream: (() => void) | null = null
  // Bumped when the stream ends, so an opener whose wire ended synchronously keeps no stale stop.
  let streamEpoch = 0
  // When the stream last said it was live; a replayed ready may have missed events since.
  let readyAt = 0
  const floorTimers = new Map<string, ReturnType<typeof setTimeout>>()
  const signalledQueries = (matches: (signal: HostQuerySignal) => boolean): Query[] =>
    queryClient.getQueryCache().findAll({
      queryKey: hostScopeKey(hostId),
      predicate: (query) => {
        const signal = signalOf(query)
        return signal !== undefined && matches(signal)
      }
    })
  // Unobserved or hidden: stale only, so the next mount or foreground reads it.
  const readsNow = (query: Query): boolean => query.isActive() && focusManager.isFocused()

  const onChange = (query: Query): void => {
    markStaleAfterInFlight(queryClient, query)
    if (!readsNow(query) || floorTimers.has(query.queryHash)) {
      return
    }
    // Change events can come per agent step; keep reads `floorMs` apart, polls included.
    const wait = query.state.dataUpdatedAt + (signalOf(query)?.floorMs ?? 0) - Date.now()
    if (wait <= 0) {
      refetchHostQueryAfterInFlight(queryClient, query)
      return
    }
    floorTimers.set(
      query.queryHash,
      setTimeout(() => {
        floorTimers.delete(query.queryHash)
        if (readsNow(query)) {
          refetchHostQueryAfterInFlight(queryClient, query)
        }
      }, wait)
    )
  }

  // Reads again (or marks stale, when no one watches) every answer older than `readSince`.
  const catchUp = (readSince: number): void => {
    for (const query of signalledQueries(() => true)) {
      if (query.state.fetchStatus === 'fetching' || query.state.dataUpdatedAt >= readSince) {
        continue
      }
      if (readsNow(query)) {
        void queryClient.invalidateQueries({ queryKey: query.queryKey, exact: true })
      } else {
        void markStale(queryClient, query)
      }
    }
  }

  const onReplayedReady = (missedSince: number): void => {
    // After a reconnect, the connected edge already re-read every observed query; a replay with no reconnect (a path switch) did not.
    const connectedAt = client.getLastConnectedAt() ?? 0
    catchUp(connectedAt > missedSince ? connectedAt : Infinity)
  }

  const onEvent = (event: SharedClientEvent): void => {
    if (event.type === 'ready') {
      // Events are not queued while disconnected, so a replayed ready may follow missed changes.
      if (!ready) {
        // Before this router started, or since its last stream ended, nothing heard this host.
        if (endedAt === null) {
          catchUp(startedAt)
        } else {
          onReplayedReady(endedAt)
        }
      } else if (!('joined' in event)) {
        onReplayedReady(readyAt)
      }
      readyAt = Date.now()
      setReady(true)
      return
    }
    if (event.type === 'end' || event.type === 'error') {
      // The shared stream is gone; the next connected edge opens a fresh one (the edge itself re-reads what was missed).
      endedAt = Date.now()
      setReady(false)
      streamEpoch += 1
      stopStream?.()
      stopStream = null
      return
    }
    for (const query of signalledQueries((signal) => signalMatches(signal, event))) {
      onChange(query)
    }
  }

  const openStream = (): void => {
    const epoch = streamEpoch
    try {
      const stop = subscribeSharedClientEvents(client, onEvent)
      if (streamEpoch === epoch) {
        stopStream = stop
      } else {
        // The wire ended while opening; the next connected edge tries again.
        stop()
      }
    } catch {
      // A page that can't open a stream now gets another chance on the next connected edge.
    }
  }
  openStream()
  const stopWatchingConnection = client.onStateChange((state) => {
    if (state === 'connected' && !stopStream) {
      openStream()
    }
  })
  return {
    stop: () => {
      stopWatchingConnection()
      stopStream?.()
      stopStream = null
      for (const timer of floorTimers.values()) {
        clearTimeout(timer)
      }
      floorTimers.clear()
      readyListeners.clear()
    },
    isReady: () => ready,
    onReadyChange: (listener) => {
      readyListeners.add(listener)
      return () => readyListeners.delete(listener)
    }
  }
}

type Router = { holders: number; handle: RouterHandle }

const routers = new WeakMap<QueryClient, Map<RpcClient, Router>>()

// One router per world per host client, however many screens read that host.
function retainHostEventRouter(
  queryClient: QueryClient,
  hostId: string,
  client: RpcClient
): { handle: RouterHandle; release: () => void } {
  let byClient = routers.get(queryClient)
  if (!byClient) {
    byClient = new Map()
    routers.set(queryClient, byClient)
  }
  const clients = byClient
  let router = clients.get(client)
  if (!router) {
    router = { holders: 0, handle: startHostEventRouter(queryClient, hostId, client) }
    clients.set(client, router)
  }
  const held = router
  held.holders += 1
  const release = (): void => {
    held.holders -= 1
    if (held.holders === 0) {
      held.handle.stop()
      clients.delete(client)
    }
  }
  return { handle: held.handle, release }
}

/** Keeps the host's event-signalled families (see HostQuery `signal`) fresh while a screen reads them. */
export function useHostEventRouter(hostId: string | undefined, client: RpcClient | null): void {
  const queryClient = useQueryClient()
  useEffect(
    () =>
      client && hostId ? retainHostEventRouter(queryClient, hostId, client).release : undefined,
    [queryClient, hostId, client]
  )
}

/**
 * Keeps the host's event router running, like useHostEventRouter, and says whether its event stream
 * is live. Only while it is can a family hold its answer until an event marks it stale.
 */
export function useHostEventStreamReady(
  hostId: string | undefined,
  client: RpcClient | null
): boolean {
  const queryClient = useQueryClient()
  const subscribe = useCallback(
    (onChange: () => void) => {
      if (!client || !hostId) {
        return () => {}
      }
      const { handle, release } = retainHostEventRouter(queryClient, hostId, client)
      const stopListening = handle.onReadyChange(onChange)
      return () => {
        stopListening()
        release()
      }
    },
    [queryClient, hostId, client]
  )
  return useSyncExternalStore(subscribe, () =>
    client && hostId ? (routers.get(queryClient)?.get(client)?.handle.isReady() ?? false) : false
  )
}

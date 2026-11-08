import { SETTINGS_CHANGED_EVENT_RUNTIME_CAPABILITY } from '../../../src/shared/protocol-version'
import { useOptionalHostProtocolGates } from '../components/host-protocol-gates-context'
import type { RpcClient } from '../transport/rpc-client'
import { useRouteRelaysCapability } from '../transport/workspace-server-context'
import { useHostEventStreamReady } from './host-event-router'

/**
 * Whether a settings family can hold its answer until a settingsChanged event marks it stale: the
 * route's host publishes the event and this page's event stream from it is live. Otherwise (older
 * hosts, a stream that ended, screens outside a host route) the family keeps its own rereads.
 */
export function useHostSettingsChangeSignal(hostId: string, client: RpcClient | null): boolean {
  const hostCapabilities = useOptionalHostProtocolGates()?.hostCapabilities
  const relayed = useRouteRelaysCapability(SETTINGS_CHANGED_EVENT_RUNTIME_CAPABILITY)
  const published =
    hostCapabilities?.includes(SETTINGS_CHANGED_EVENT_RUNTIME_CAPABILITY) === true && relayed
  const streamReady = useHostEventStreamReady(
    published ? hostId : undefined,
    published ? client : null
  )
  return published && streamReady
}

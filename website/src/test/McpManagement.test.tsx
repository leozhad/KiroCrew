// MCP Management: the states where the page could lie to the operator.
//
// Contract under test:
// - a stub apply that PERSISTED but did not go live (200 with applied:false)
//   surfaces an error instead of drawing a live-looking switch
// - a failed server request renders its own error row, never the "none are
//   configured" empty state, which is a claim a failed request cannot make
// - an unsupported platform can still turn an inherited setting OFF; only
//   turning one ON is blocked, so nobody is trapped in a state they cannot exit
// - sharing cannot be enabled while nothing is stubbed (it would do nothing),
//   but can always be disabled
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, cleanup, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { McpManagement } from '../pages/settings/McpManagement'
import { api } from '../api/client'

type Server = {
  name: string
  stub: boolean
  can_stub: boolean
  in_allowlist: boolean
  entry_poolable: boolean
  agents: string[]
  transport: string
  denylisted: boolean
}

function server(over: Partial<Server> = {}): Server {
  return {
    name: 'alpha-mcp',
    stub: false,
    can_stub: true,
    in_allowlist: false,
    entry_poolable: false,
    agents: ['kirocrew'],
    transport: 'stdio',
    denylisted: false,
    ...over,
  }
}

function mount() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={qc}>
      <McpManagement />
    </QueryClientProvider>,
  )
}

const status = (over: Record<string, unknown> = {}) => ({
  enabled: false,
  stub: [] as string[],
  stub_count: 0,
  running: false,
  ping_ok: false,
  supported: true,
  ...over,
})

beforeEach(() => {
  vi.restoreAllMocks()
})
afterEach(cleanup)

describe('McpManagement', () => {
  it('reports a stub that saved but did not come up', async () => {
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(status() as never)
    vi.spyOn(api, 'mcpGatewayServers').mockResolvedValue({ servers: [server()] } as never)
    // The broker failed to start: the endpoint still answers 200.
    vi.spyOn(api, 'mcpGatewaySetStub').mockResolvedValue({
      ok: true,
      name: 'alpha-mcp',
      stub: true,
      applied: false,
    } as never)

    mount()
    const row = await screen.findByRole('switch', { name: /alpha-mcp/i })
    row.click()

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeTruthy()
    })
  })

  it('does not claim zero servers when the request failed', async () => {
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(status() as never)
    vi.spyOn(api, 'mcpGatewayServers').mockRejectedValue(new Error('boom'))

    mount()
    await waitFor(() => {
      expect(screen.queryByText(/no mcp servers are configured/i)).toBeNull()
      expect(screen.getByText(/could not load the server list/i)).toBeTruthy()
    })
  })

  it('lets an unsupported platform turn an inherited stub back off', async () => {
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(
      status({ supported: false, enabled: true, stub_count: 1 }) as never,
    )
    vi.spyOn(api, 'mcpGatewayServers').mockResolvedValue({
      servers: [server({ stub: true, in_allowlist: true })],
    } as never)

    mount()
    const row = await screen.findByRole('switch', { name: /alpha-mcp/i })
    // ON and unsupported: turning it OFF must stay reachable.
    await waitFor(() => expect((row as HTMLButtonElement).disabled).toBe(false))

    const sharing = screen.getByRole('switch', { name: /share backends/i })
    await waitFor(() => expect((sharing as HTMLButtonElement).disabled).toBe(false))
  })

  it('blocks enabling a stub on an unsupported platform', async () => {
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(status({ supported: false }) as never)
    vi.spyOn(api, 'mcpGatewayServers').mockResolvedValue({ servers: [server()] } as never)

    mount()
    const row = await screen.findByRole('switch', { name: /alpha-mcp/i })
    expect((row as HTMLButtonElement).disabled).toBe(true)
  })

  it('refetches after a failed apply, because the setting may already be saved', async () => {
    // Both endpoints write config.json BEFORE the in-process apply, so a 500 is
    // "saved but not live". Leaving the old state on screen and saying nothing
    // was saved would hide a setting that activates on the next restart.
    const statusSpy = vi
      .spyOn(api, 'mcpGatewayStatus')
      .mockResolvedValue(status({ stub_count: 1 }) as never)
    vi.spyOn(api, 'mcpGatewayServers').mockResolvedValue({
      servers: [server({ stub: true, in_allowlist: true })],
    } as never)
    vi.spyOn(api, 'mcpGatewayEnable').mockRejectedValue(new Error('apply failed'))

    mount()
    const sharing = await screen.findByRole('switch', { name: /share backends/i })
    await waitFor(() => expect((sharing as HTMLButtonElement).disabled).toBe(false))
    const before = statusSpy.mock.calls.length
    sharing.click()

    // The confirm dialog guards enabling; take it.
    const confirm = await screen.findByRole('button', { name: /turn on sharing/i })
    confirm.click()

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeTruthy()
      expect(statusSpy.mock.calls.length).toBeGreaterThan(before)
    })
    expect(screen.getByRole('alert').textContent ?? '').not.toMatch(/nothing was saved/i)
  })

  it('refuses to arm sharing while nothing is stubbed, but allows disarming it', async () => {
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(status() as never)
    vi.spyOn(api, 'mcpGatewayServers').mockResolvedValue({ servers: [server()] } as never)

    const { unmount } = mount()
    const off = await screen.findByRole('switch', { name: /share backends/i })
    await waitFor(() => expect((off as HTMLButtonElement).disabled).toBe(true))
    unmount()
    cleanup()

    // Already on with nothing stubbed — the state this PR removes. It must still
    // be escapable, or the operator is stuck with a switch they cannot clear.
    vi.spyOn(api, 'mcpGatewayStatus').mockResolvedValue(status({ enabled: true }) as never)
    mount()
    const on = await screen.findByRole('switch', { name: /share backends/i })
    // Poll: the switch mounts before the status query resolves, and until it does
    // the page cannot know the setting is already on.
    await waitFor(() => expect((on as HTMLButtonElement).disabled).toBe(false))
  })
})

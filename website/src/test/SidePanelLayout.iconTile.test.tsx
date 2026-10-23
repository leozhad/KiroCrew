/**
 * Settings section icons sit on a coloured rounded tile, the way macOS / iOS
 * System Settings mark each section. The tile is opt-in per tab (`tile`, a hue
 * name): the other hosts of this layout pass no tile and keep the bare `--muted`
 * icon, so a tile must never appear for a tab that did not ask. Both branches
 * render it — the mobile root list and the desktop rail — because a section's
 * mark is its identity and has to be the same on both.
 *
 * The built-in themes paint the fixed system hues; a custom theme maps every hue
 * onto one of its own palette roles, so an installed pack's Settings rail follows
 * the pack.
 */
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import SidePanelLayout, { type SidePanelTab } from '../components/SidePanelLayout'
import { ALLOWED_CSS_VARS } from '../hooks/themeCss'
import { readIndexCss } from './indexCssSource'

let mobile = true
vi.mock('../hooks/useIsMobile', () => ({ useIsMobile: () => mobile }))

const INDEX_CSS = readIndexCss()

const TABS: SidePanelTab[] = [
  { key: 'chat', label: 'Chat', icon: <svg data-testid="chat-icon" />, tile: 'green' },
  { key: 'about', label: 'About', icon: <svg data-testid="about-icon" /> },
]

function renderAt(search: string) {
  return render(
    <MemoryRouter initialEntries={[`/settings${search}`]}>
      <SidePanelLayout title="Settings" tabs={TABS} rememberKey="test-icon-tile">
        {tab => <div data-testid="pane">{tab}</div>}
      </SidePanelLayout>
    </MemoryRouter>,
  )
}

const tileOf = (iconTestId: string) => screen.getByTestId(iconTestId).parentElement as HTMLElement

/** The declarations of the one rule whose selector is exactly `selector`. */
function block(selector: string): Map<string, string> {
  const at = INDEX_CSS.indexOf(`\n${selector} {`)
  expect(at, `${selector} block in index.css`).toBeGreaterThan(-1)
  const body = INDEX_CSS.slice(INDEX_CSS.indexOf('{', at) + 1, INDEX_CSS.indexOf('}', at))
  return new Map(
    body.split(';').map(d => d.trim()).filter(Boolean).map(d => {
      const i = d.indexOf(':')
      return [d.slice(0, i).trim(), d.slice(i + 1).trim()] as [string, string]
    }),
  )
}

const settingsHues = () => {
  const settingsPage = readFileSync(resolve(process.cwd(), 'src/pages/SettingsPage.tsx'), 'utf-8')
  return new Set([...settingsPage.matchAll(/\btile: '([a-z]+)'/g)].map(m => m[1]))
}

describe('settings icon tiles', () => {
  beforeEach(() => { sessionStorage.clear() })
  afterEach(() => { vi.restoreAllMocks(); cleanup() })

  it('paints the tile behind a tab that asks for one on the mobile root list, and only there', () => {
    mobile = true
    renderAt('')
    expect(tileOf('chat-icon').style.background).toBe('var(--tile-green)')
    expect(tileOf('chat-icon').style.color).toBe('var(--tile-green-fg)')
    expect(tileOf('about-icon').style.background).toBe('')
    expect(tileOf('about-icon').className).toContain('text-muted')
  })

  it('paints the same tile on the desktop rail', () => {
    mobile = false
    renderAt('?tab=chat')
    expect(tileOf('chat-icon').style.background).toBe('var(--tile-green)')
    expect(tileOf('chat-icon').style.color).toBe('var(--tile-green-fg)')
    expect(tileOf('about-icon').style.background).toBe('')
  })

  // A built-in theme keeps the fixed system hues: one `:root` block and one
  // `[data-mode="light"]` block, both carrying every hue the Settings tabs use,
  // and one `--tile-fg` ink behind every hue's glyph.
  it('defines every hue the Settings tabs use for both built-in polarities', () => {
    const used = settingsHues()
    expect(used.size).toBeGreaterThan(0)
    const root = INDEX_CSS.match(/:root \{\s*--tile-fg:[\s\S]*?\}/)?.[0] ?? ''
    const light = INDEX_CSS.match(/\[data-mode="light"\] \{\s*--tile-gray:[\s\S]*?\}/)?.[0] ?? ''
    expect(root).toContain('--tile-fg:')
    for (const hue of used) {
      expect(root, `--tile-${hue} in :root`).toContain(`--tile-${hue}:`)
      expect(root, `--tile-${hue}-fg in :root`).toContain(`--tile-${hue}-fg: var(--tile-fg);`)
      expect(light, `--tile-${hue} in light`).toContain(`--tile-${hue}:`)
    }
  })

  // A custom theme is the palette a pack chose, so its tiles follow it. Every
  // hue reads a role a pack can set (the allowlist), and its glyph reads that
  // SAME role's `-fg` ink, the token a pack sets for text on that fill.
  it('maps every hue onto a pack-settable palette role and its own ink in a custom theme', () => {
    const custom = block('[data-theme^="custom-"]')
    for (const hue of settingsHues()) {
      const fill = /^var\((--[a-z-]+)\)$/.exec(custom.get(`--tile-${hue}`) ?? '')?.[1]
      expect(fill, `--tile-${hue} reads a palette role`).toBeDefined()
      expect(ALLOWED_CSS_VARS.has(fill!), `${fill} is pack-settable`).toBe(true)
      expect(custom.get(`--tile-${hue}-fg`), `--tile-${hue}-fg`).toBe(`var(${fill}-fg)`)
      expect(ALLOWED_CSS_VARS.has(`${fill}-fg`), `${fill}-fg is pack-settable`).toBe(true)
    }
  })

  // The custom block and the light block have equal specificity, so the custom
  // one only wins in a light custom theme because it comes later.
  it('places the custom-theme mapping after the built-in light hues', () => {
    const light = INDEX_CSS.search(/\[data-mode="light"\] \{\s*--tile-gray:/)
    const custom = INDEX_CSS.indexOf('\n[data-theme^="custom-"] {')
    expect(light).toBeGreaterThan(-1)
    expect(custom).toBeGreaterThan(light)
  })
})

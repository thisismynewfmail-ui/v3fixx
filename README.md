# CHATSGI / TERMINAL

```
┌──────────────────────────────────────────────┐
│  Section 9  ·  rev. 2.0                      │
│  Stand Alone Complex · Maintenance Terminal  │
└──────────────────────────────────────────────┘
```

A local maintenance terminal for an OpenAI-compatible LLM endpoint with full
**MCP (Model Context Protocol)** support and a Ghost in the Shell: S.A.C.
(2002) themed UI. Python backend spawns MCP servers as stdio subprocesses
(the Claude-Desktop pattern); a single-page browser UI talks to it over HTTP,
with a Server-Sent-Events bus that keeps multiple browser instances mirrored
in real time across the LAN.

## ◢ Files

| file                | purpose                                              |
|---------------------|------------------------------------------------------|
| `start.py`          | HTTP server + MCP stdio bridge + SSE event bus       |
| `index.html`        | UI, served by `start.py` at `/`                      |
| `tools.json`        | MCP server config (Claude-Desktop format)            |
| `settings.json`     | persisted UI settings (auto-created)                 |
| `data/`             | per-session chat files (one `.json` per chat) + `index.json` |
| `chats.json`        | legacy bundled history — migrated into `data/` on boot |
| `conversation.json` | legacy single-chat log — migrated into `data/`       |
| `requirements.txt`  | empty (stdlib only — kept for transparency)          |

## ◢ Run

```bash
python start.py
```

Defaults bind to `127.0.0.1:8765`. Toggle **§ 7 NETWORK → LAN VISIBILITY** in
the UI to bind `0.0.0.0` on the next launch, or override directly:

```bash
python start.py --host 0.0.0.0 --port 9000   # custom bind / LAN visibility
python start.py --no-mcp                      # don't auto-spawn MCP servers
```

Requires Python ≥ 3.10. No `pip install` needed — stdlib only.

## ◢ Features

- **Multi-chat sessions** — left sidebar lists every chat; each session lives
  in its own file under `data/`. Consolidated `▾ MENU` button (New / Rename /
  Duplicate / Copy / Export / Delete the active chat) and per-row `⋮` menu for
  the same actions on any other chat. Inline rename (click ▾ MENU → RENAME).
- **Collapsible side panels** — toggle the left chat list and the right
  options/MCP/telemetry panel independently using the edge buttons on the
  main pane. Collapsed state persists across reloads.
- **Cross-instance sync** — every browser/window subscribes to a Server-Sent
  Events stream. Send a message on one monitor and it appears on every other
  connected viewer immediately. Settings, chat list, active selection, and
  MCP status all mirror.
- **Rolling context window** — when token usage crosses the threshold the
  oldest middle messages fall out of the window until usage drops below the
  cap. The first user message (original task) and the most recent turns are
  always preserved. Tool-call / tool-result pairs drop atomically so the
  trace stays coherent. The `ROLL` status pill in the bottom status bar
  spins while it runs; a compact marker is left in the chat where the cut
  happened. Each tool round gets a fresh `max_tokens` budget so tool calls
  are not counted against the output-token cap.
- **Per-MCP-server toggle** — flip an individual server on/off from the right
  panel. Disabled servers are not started and their tools are withheld from
  the model. Persists in `settings.json` (`mcpEnabled`).
- **Collapsed tool calls** — every tool call and tool result is collapsed by
  default; click the header to expand. Toggle the default in settings.
- **Connection retry** — the LLM proxy retries transient network failures
  (and 5xx responses) with exponential backoff, both server-side and
  (for direct-mode) client-side.
- **Sampling controls** — temperature, top_p, top_k, min_p, repeat_penalty,
  frequency_penalty, presence_penalty, seed, stop sequences. Extended params
  are sent both top-level and inside `extra_body` for llama.cpp / ollama /
  koboldcpp compatibility.
- **Tool-call recursion limit** — configurable cap on consecutive tool rounds
  before the agent must finalise a text reply.
- **Villager-talk SFX** — Animal Crossing-style synthesised blip per streamed
  character (WebAudio, no asset dependency). Toggle, volume, and pitch in
  settings.

## ◢ HTTP API

Used by the UI; usable directly if you want to script it.

| route                                | what it does                                |
|--------------------------------------|---------------------------------------------|
| `GET  /`                             | serves `index.html`                         |
| `GET  /api/config`                   | returns `settings.json`                     |
| `POST /api/config`                   | merges + writes `settings.json`             |
| `GET  /api/tools-config`             | returns `tools.json`                        |
| `POST /api/tools-config`             | writes `tools.json` and respawns MCP        |
| `GET  /api/tools`                    | lists OpenAI-format tools from running MCP  |
| `GET  /api/servers`                  | per-server status, recent stderr, errors    |
| `POST /api/servers/reload`           | respawns all MCP servers                    |
| `POST /api/tools/call`               | `{name, arguments}` → MCP `tools/call`      |
| `GET  /api/chats`                    | list chats + active id                      |
| `POST /api/chats`                    | create chat                                 |
| `GET  /api/chats/{id}`               | fetch full chat                             |
| `POST /api/chats/{id}`               | update messages / name                      |
| `DELETE /api/chats/{id}`             | delete chat                                 |
| `POST /api/chats/{id}/rename`        | rename                                      |
| `POST /api/chats/{id}/duplicate`     | duplicate                                   |
| `POST /api/chats/active`             | set active chat id                          |
| `GET  /api/events`                   | Server-Sent Events bus (sync)               |
| `POST /api/llm/proxy`                | streaming proxy to the OpenAI endpoint      |
| `GET  /api/health`                   | liveness                                    |

## ◢ tools.json

Standard MCP config — same shape Claude Desktop uses. Each entry under
`mcpServers` is keyed by display name and specifies the executable to launch.
Each server must be on `PATH` and speak MCP over stdio.

## ◢ Aesthetic — rev. 3.0 BIOLAB

Phosphor-green CRT palette with magenta instrument accents. Bracketed
"specimen card" frames, dithered backplate, animated radar reticle with a
sweeping magenta wedge. Dynamic readouts wired live to the session:

- **SPECIMEN CARD** (right panel) — the active chat surfaced as a lab
  specimen with TYPE, SIZE, CONDITION, SIGNAL, RARITY, TOKENS. Condition
  inverts capacity utilisation; rarity escalates with conversation depth.
  An 18-cell capacity bar lights green → amber → critical as the rolling
  context fills.
- **VITALITY** strip — per-message token bars, colour-coded by role
  (amber=user, green=assistant, magenta=tool). Streaming bars pulse.
- **CARGO HOLD** — every loaded MCP tool rendered as a pixel cell with a
  deterministic glyph; hover reveals its server / description.

Visual debt to *Ghost in the Shell: SAC* Section 9 HUDs and bio-industrial
trading-terminal UIs — dense, labelled, instrumental.

```
A stand-alone complex is a phenomenon by which unrelated copycats are mistakenly
thought to be an organised conspiracy.
```

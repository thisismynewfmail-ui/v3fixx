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
| `chats.json`        | multi-chat history store (auto-created / migrated)   |
| `conversation.json` | legacy single-chat log — migrated into `chats.json`  |
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

- **Multi-chat sessions** — left sidebar lists every chat; rename, duplicate,
  copy-to-clipboard, export, delete from the per-row menu. The active chat is
  highlighted; all chats persist to disk.
- **Cross-instance sync** — every browser/window subscribes to a Server-Sent
  Events stream. Send a message on one monitor and it appears on every other
  connected viewer immediately. Settings, chat list, active selection, and
  MCP status all mirror.
- **Background compaction** — when context usage crosses the threshold the
  buffer is recompacted in the background. The chat UI is *not* polluted with
  banners; the `CMP` status pill in the bottom status bar spins while it
  runs.
- **Collapsed tool calls** — every tool call and tool result is collapsed by
  default; click the header to expand. Toggle the default in settings.
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

## ◢ Aesthetic

Cool teal/cyan HUD palette, Share Tech Mono + Chakra Petch, slow rotating
target reticle, subtle scanlines and roaming sweep line. Visual debt to the
*Ghost in the Shell: Stand Alone Complex* (2002) tachikoma diagnostic
displays and Section 9 mission HUDs — dense, labelled, instrumental.

```
A stand-alone complex is a phenomenon by which unrelated copycats are mistakenly
thought to be an organised conspiracy.
```

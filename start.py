#!/usr/bin/env python3
"""
CHATSGI / TERMINAL — backend
Section 9 · stdlib-only HTTP server + MCP stdio bridge + LAN sync

Spawns MCP servers from tools.json, exposes them over HTTP for the browser UI,
persists settings + multi-chat history, proxies LLM requests, and broadcasts
state changes over Server-Sent Events so multiple browser instances mirror
each other in real time.

    python start.py [--host 0.0.0.0] [--port 8765] [--no-mcp]
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn

# ───────────────────────────── Paths ─────────────────────────────

ROOT = Path(__file__).resolve().parent
SETTINGS_PATH = ROOT / "settings.json"
TOOLS_PATH = ROOT / "tools.json"
INDEX_PATH = ROOT / "index.html"
CONV_PATH = ROOT / "conversation.json"   # legacy — migrated on boot
CHATS_PATH = ROOT / "chats.json"

DEFAULT_SETTINGS = {
    "endpoint": "http://localhost:8080/v1",
    "apiKey": "",
    "model": "",
    "temperature": 0.7,
    "maxTokens": 2048,
    "context": 64200,
    "threshold": 70,
    "systemPrompt": (
        "You are a precise, helpful AI agent operating inside the CHATSGI "
        "terminal. Use the MCP tools provided when they help; report your "
        "work clearly and cite tool results."
    ),
    "detectedPrompt": "",
    "thinkingEnabled": False,
    "thinkCollapsed": True,
    "streaming": True,
    "toolsEnabled": True,
    "toolsCollapsed": True,
    "useProxy": True,
    "soundEnabled": True,
    "soundVolume": 0.35,
    "soundPitch": 1.0,
    "networkVisible": False,
    "maxToolRecursion": 16,
    "minP": 0.0,
    "topP": 1.0,
    "topK": 0,
    "repeatPenalty": 1.0,
    "frequencyPenalty": 0.0,
    "presencePenalty": 0.0,
    "stopSequences": "",
    "seed": 0,
}

# ─────────────────────────── MCP Client ──────────────────────────

class MCPClient:
    """One stdio-connected MCP server. JSON-RPC 2.0 over line-delimited JSON."""

    def __init__(self, name, command, args, env=None, cwd=None):
        self.name = name
        self.command = command
        self.args = list(args or [])
        self.env = dict(env or {})
        self.cwd = cwd
        self.proc: subprocess.Popen | None = None
        self._write_lock = threading.Lock()
        self._req_id = 0
        self._pending: dict[int, tuple[threading.Event, dict]] = {}
        self.tools: list[dict] = []
        self.status = "stopped"        # stopped|starting|online|error|crashed
        self.error: str | None = None
        self.recent_stderr: list[str] = []
        self._reader: threading.Thread | None = None
        self._errreader: threading.Thread | None = None

    def start(self) -> bool:
        try:
            self.status = "starting"
            self.error = None
            full_env = os.environ.copy()
            full_env.update(self.env)
            cmd = shutil.which(self.command) or self.command
            self.proc = subprocess.Popen(
                [cmd, *self.args],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=full_env,
                cwd=self.cwd,
                bufsize=0,
            )
            self._reader = threading.Thread(target=self._read_loop, name=f"mcp-{self.name}-out", daemon=True)
            self._reader.start()
            self._errreader = threading.Thread(target=self._stderr_loop, name=f"mcp-{self.name}-err", daemon=True)
            self._errreader.start()

            self._call("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "chatsgi-terminal", "version": "2.0"},
            }, timeout=20)
            self._notify("notifications/initialized", {})

            tools_resp = self._call("tools/list", {}, timeout=15) or {}
            self.tools = tools_resp.get("tools", [])
            self.status = "online"
            return True
        except Exception as e:
            self.status = "error"
            self.error = f"{type(e).__name__}: {e}"
            self._stop_quiet()
            return False

    def stop(self) -> None:
        self._stop_quiet()
        self.status = "stopped"

    def _stop_quiet(self) -> None:
        if self.proc:
            try:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except Exception:
                pass
        self.proc = None

    def _send(self, obj: dict) -> None:
        if not self.proc or self.proc.poll() is not None:
            raise RuntimeError(f"server '{self.name}' not running")
        line = (json.dumps(obj) + "\n").encode("utf-8")
        with self._write_lock:
            assert self.proc.stdin is not None
            self.proc.stdin.write(line)
            self.proc.stdin.flush()

    def _call(self, method: str, params=None, timeout: float = 30.0):
        self._req_id += 1
        rid = self._req_id
        ev = threading.Event()
        box = {"result": None, "error": None}
        self._pending[rid] = (ev, box)
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
        if not ev.wait(timeout):
            self._pending.pop(rid, None)
            raise TimeoutError(f"{self.name}: no response to {method} in {timeout}s")
        self._pending.pop(rid, None)
        if box["error"]:
            raise RuntimeError(f"{self.name}.{method}: {box['error']}")
        return box["result"]

    def _notify(self, method: str, params=None) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def _read_loop(self) -> None:
        try:
            assert self.proc and self.proc.stdout
            while True:
                line = self.proc.stdout.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode("utf-8", errors="replace"))
                except Exception:
                    continue
                if "id" in msg and msg["id"] in self._pending:
                    ev, box = self._pending[msg["id"]]
                    if "error" in msg:
                        box["error"] = msg["error"]
                    else:
                        box["result"] = msg.get("result")
                    ev.set()
        except Exception:
            pass
        finally:
            if self.status == "online":
                self.status = "crashed"

    def _stderr_loop(self) -> None:
        try:
            assert self.proc and self.proc.stderr
            while True:
                line = self.proc.stderr.readline()
                if not line:
                    break
                txt = line.decode("utf-8", errors="replace").rstrip()
                self.recent_stderr.append(txt)
                if len(self.recent_stderr) > 200:
                    self.recent_stderr = self.recent_stderr[-200:]
        except Exception:
            pass

    def call_tool(self, tool_name: str, arguments: dict, timeout: float = 180.0):
        return self._call("tools/call", {"name": tool_name, "arguments": arguments or {}}, timeout=timeout)


# ────────────────────────── MCP Manager ──────────────────────────

class MCPManager:
    def __init__(self) -> None:
        self.clients: dict[str, MCPClient] = {}
        self._lock = threading.Lock()

    def load(self, config: dict) -> None:
        with self._lock:
            for c in self.clients.values():
                c.stop()
            self.clients = {}
            servers = (config or {}).get("mcpServers", {}) or {}
            for name, cfg in servers.items():
                if not isinstance(cfg, dict) or not cfg.get("command"):
                    continue
                client = MCPClient(
                    name=name,
                    command=cfg.get("command"),
                    args=cfg.get("args", []),
                    env=cfg.get("env"),
                    cwd=cfg.get("cwd"),
                )
                self.clients[name] = client
            for client in list(self.clients.values()):
                threading.Thread(target=self._start_then_publish,
                                 args=(client,),
                                 name=f"mcp-start-{client.name}", daemon=True).start()

    def _start_then_publish(self, client: MCPClient):
        client.start()
        BUS.publish("mcp", {"servers": self.status_summary(), "tools": self.list_tools()})

    def stop_all(self) -> None:
        with self._lock:
            for c in self.clients.values():
                c.stop()

    def list_tools(self) -> list[dict]:
        out = []
        with self._lock:
            clients = list(self.clients.values())
        for c in clients:
            for t in c.tools:
                tname = t.get("name", "")
                full = f"{c.name}__{tname}"
                out.append({
                    "type": "function",
                    "function": {
                        "name": full,
                        "description": t.get("description", "") or "",
                        "parameters": t.get("inputSchema") or {"type": "object", "properties": {}},
                    },
                    "_server": c.name,
                    "_original_name": tname,
                })
        return out

    def status_summary(self) -> list[dict]:
        with self._lock:
            clients = list(self.clients.values())
        return [{
            "name": c.name,
            "status": c.status,
            "tool_count": len(c.tools),
            "command": c.command,
            "args": c.args,
            "tools": [t.get("name") for t in c.tools],
            "error": c.error,
            "stderr_tail": c.recent_stderr[-6:] if c.recent_stderr else [],
        } for c in clients]

    def call(self, full_name: str, arguments: dict):
        if "__" in full_name:
            server, tool = full_name.split("__", 1)
            client = self.clients.get(server)
            if not client:
                raise ValueError(f"unknown server '{server}'")
        else:
            client = None
            tool = full_name
            for c in self.clients.values():
                if any(t.get("name") == full_name for t in c.tools):
                    client = c
                    break
            if client is None:
                raise ValueError(f"no MCP server exposes tool '{full_name}'")
        if client.status != "online":
            raise RuntimeError(f"server '{client.name}' not online (status={client.status})")
        return client.call_tool(tool, arguments)


# ─────────────────────── Event Bus (SSE) ─────────────────────────

class EventBus:
    """Fan-out of state-change events to every connected SSE client."""

    def __init__(self):
        self._lock = threading.Lock()
        self._subs: set[queue.Queue] = set()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=128)
        with self._lock:
            self._subs.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            self._subs.discard(q)

    def publish(self, kind: str, payload: dict, origin: str | None = None) -> None:
        evt = {"kind": kind, "ts": int(time.time() * 1000), "origin": origin, "payload": payload}
        try:
            data = json.dumps(evt)
        except Exception:
            return
        with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(data)
            except queue.Full:
                pass


BUS = EventBus()


# ────────────────────────── Persistence ──────────────────────────

_PERSIST_LOCK = threading.RLock()

def load_settings() -> dict:
    with _PERSIST_LOCK:
        if SETTINGS_PATH.exists():
            try:
                return {**DEFAULT_SETTINGS, **json.loads(SETTINGS_PATH.read_text("utf-8"))}
            except Exception:
                pass
        return dict(DEFAULT_SETTINGS)

def save_settings(s: dict) -> None:
    with _PERSIST_LOCK:
        SETTINGS_PATH.write_text(json.dumps(s, indent=2), "utf-8")

def load_tools_config() -> dict:
    with _PERSIST_LOCK:
        if TOOLS_PATH.exists():
            try:
                return json.loads(TOOLS_PATH.read_text("utf-8"))
            except Exception:
                pass
        return {"mcpServers": {}}

def save_tools_config(c: dict) -> None:
    with _PERSIST_LOCK:
        TOOLS_PATH.write_text(json.dumps(c, indent=2), "utf-8")

def _new_id() -> str:
    return secrets.token_hex(8)

def _default_chats() -> dict:
    cid = _new_id()
    return {
        "active": cid,
        "chats": [{
            "id": cid,
            "name": "Session 01",
            "created": int(time.time() * 1000),
            "updated": int(time.time() * 1000),
            "messages": [],
        }],
    }

def load_chats() -> dict:
    with _PERSIST_LOCK:
        if CHATS_PATH.exists():
            try:
                v = json.loads(CHATS_PATH.read_text("utf-8"))
                if isinstance(v, dict) and isinstance(v.get("chats"), list) and v["chats"]:
                    return v
            except Exception:
                pass
        # legacy migration
        if CONV_PATH.exists():
            try:
                msgs = json.loads(CONV_PATH.read_text("utf-8"))
                if isinstance(msgs, list):
                    cid = _new_id()
                    blob = {
                        "active": cid,
                        "chats": [{
                            "id": cid,
                            "name": "Imported Session",
                            "created": int(time.time() * 1000),
                            "updated": int(time.time() * 1000),
                            "messages": msgs,
                        }],
                    }
                    CHATS_PATH.write_text(json.dumps(blob, indent=2), "utf-8")
                    return blob
            except Exception:
                pass
        blob = _default_chats()
        CHATS_PATH.write_text(json.dumps(blob, indent=2), "utf-8")
        return blob

def save_chats(blob: dict) -> None:
    with _PERSIST_LOCK:
        CHATS_PATH.write_text(json.dumps(blob, indent=2), "utf-8")

def get_chat(blob: dict, cid: str) -> dict | None:
    for c in blob.get("chats", []):
        if c.get("id") == cid:
            return c
    return None


# ───────────────────────────── HTTP ─────────────────────────────

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Origin-Id",
}

class Handler(BaseHTTPRequestHandler):
    server_version = "CHATSGI/2.0"

    def log_message(self, fmt, *args):
        # quieter — drop /api/events polling
        if "/api/events" in (args[0] if args else ""):
            return
        sys.stderr.write(f"  · {self.log_date_time_string()}  {fmt % args}\n")

    # ── helpers ──

    def _hdr(self, code: int, ct: str, length: int | None = None, extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        if length is not None:
            self.send_header("Content-Length", str(length))
        for k, v in CORS.items():
            self.send_header(k, v)
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()

    def _send_json(self, code: int, obj):
        body = json.dumps(obj).encode("utf-8")
        self._hdr(code, "application/json", len(body))
        self.wfile.write(body)

    def _send_text(self, code: int, text: str, ct: str = "text/plain; charset=utf-8"):
        body = text.encode("utf-8")
        self._hdr(code, ct, len(body))
        self.wfile.write(body)

    def _read_body(self):
        l = int(self.headers.get("Content-Length", "0") or "0")
        if l <= 0:
            return None
        raw = self.rfile.read(l)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return raw.decode("utf-8", errors="replace")

    def _origin(self) -> str | None:
        return self.headers.get("X-Origin-Id")

    def do_OPTIONS(self):
        self.send_response(204)
        for k, v in CORS.items():
            self.send_header(k, v)
        self.end_headers()

    # ── GET ──

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            if path in ("/", "/index.html"):
                if INDEX_PATH.exists():
                    self._send_text(200, INDEX_PATH.read_text("utf-8"), "text/html; charset=utf-8")
                else:
                    self._send_text(500, "index.html missing next to start.py")
                return
            if path == "/api/config":
                return self._send_json(200, load_settings())
            if path == "/api/tools-config":
                return self._send_json(200, load_tools_config())
            if path == "/api/tools":
                return self._send_json(200, {"tools": MGR.list_tools()})
            if path == "/api/servers":
                return self._send_json(200, {"servers": MGR.status_summary()})
            if path == "/api/chats":
                return self._send_json(200, self._chats_index())
            if path.startswith("/api/chats/"):
                cid = path[len("/api/chats/"):]
                blob = load_chats()
                chat = get_chat(blob, cid)
                if not chat:
                    return self._send_json(404, {"error": "no such chat"})
                return self._send_json(200, {"chat": chat})
            # legacy single-conversation alias
            if path == "/api/conversation":
                blob = load_chats()
                chat = get_chat(blob, blob.get("active")) or {}
                return self._send_json(200, {"messages": chat.get("messages", [])})
            if path == "/api/health":
                return self._send_json(200, {"ok": True, "version": "2.0", "servers": len(MGR.clients)})
            if path == "/api/events":
                return self._sse_stream()
            self._send_text(404, "not found")
        except Exception as e:
            self._send_json(500, {"error": str(e), "trace": traceback.format_exc()})

    # ── POST ──

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            body = self._read_body()
            origin = self._origin()
            if path == "/api/config":
                merged = {**load_settings(), **(body or {})}
                save_settings(merged)
                BUS.publish("settings", merged, origin=origin)
                return self._send_json(200, merged)
            if path == "/api/tools-config":
                cfg = body if isinstance(body, dict) else (json.loads(body) if isinstance(body, str) else {})
                save_tools_config(cfg)
                MGR.load(cfg)
                BUS.publish("tools-config", cfg, origin=origin)
                return self._send_json(200, {"ok": True, "servers": MGR.status_summary()})
            if path == "/api/servers/reload":
                MGR.load(load_tools_config())
                return self._send_json(200, {"ok": True, "servers": MGR.status_summary()})
            if path == "/api/tools/call":
                name = (body or {}).get("name")
                args = (body or {}).get("arguments", {}) or {}
                if not name:
                    return self._send_json(400, {"ok": False, "error": "missing 'name'"})
                result = MGR.call(name, args)
                text = self._flatten_mcp_result(result)
                return self._send_json(200, {"ok": True, "result": text, "raw": result})
            if path == "/api/llm/proxy":
                return self._proxy_llm(body or {})
            if path == "/api/chats":
                # create
                blob = load_chats()
                cid = _new_id()
                name = ((body or {}).get("name") or f"Session {len(blob['chats'])+1:02d}").strip() or "Untitled"
                chat = {
                    "id": cid,
                    "name": name,
                    "created": int(time.time() * 1000),
                    "updated": int(time.time() * 1000),
                    "messages": (body or {}).get("messages") or [],
                }
                blob["chats"].insert(0, chat)
                blob["active"] = cid
                save_chats(blob)
                BUS.publish("chats", self._chats_index(), origin=origin)
                return self._send_json(200, {"chat": chat, "index": self._chats_index()})
            if path == "/api/chats/active":
                cid = (body or {}).get("id")
                blob = load_chats()
                if not get_chat(blob, cid):
                    return self._send_json(404, {"error": "no such chat"})
                blob["active"] = cid
                save_chats(blob)
                BUS.publish("chats", self._chats_index(), origin=origin)
                return self._send_json(200, {"ok": True, "active": cid})
            if path.startswith("/api/chats/") and path.endswith("/rename"):
                cid = path[len("/api/chats/"):-len("/rename")]
                blob = load_chats()
                chat = get_chat(blob, cid)
                if not chat: return self._send_json(404, {"error": "no such chat"})
                new = ((body or {}).get("name") or "").strip()
                if new:
                    chat["name"] = new[:120]
                    chat["updated"] = int(time.time() * 1000)
                    save_chats(blob)
                    BUS.publish("chats", self._chats_index(), origin=origin)
                return self._send_json(200, {"chat": chat})
            if path.startswith("/api/chats/") and path.endswith("/duplicate"):
                cid = path[len("/api/chats/"):-len("/duplicate")]
                blob = load_chats()
                src = get_chat(blob, cid)
                if not src: return self._send_json(404, {"error": "no such chat"})
                new = {
                    "id": _new_id(),
                    "name": (src.get("name") or "Session") + " (copy)",
                    "created": int(time.time() * 1000),
                    "updated": int(time.time() * 1000),
                    "messages": list(src.get("messages", [])),
                }
                blob["chats"].insert(0, new)
                save_chats(blob)
                BUS.publish("chats", self._chats_index(), origin=origin)
                return self._send_json(200, {"chat": new, "index": self._chats_index()})
            if path.startswith("/api/chats/"):
                cid = path[len("/api/chats/"):]
                blob = load_chats()
                chat = get_chat(blob, cid)
                if not chat: return self._send_json(404, {"error": "no such chat"})
                payload = body or {}
                if "name" in payload:
                    chat["name"] = (str(payload["name"]).strip() or chat["name"])[:120]
                if "messages" in payload and isinstance(payload["messages"], list):
                    chat["messages"] = payload["messages"]
                chat["updated"] = int(time.time() * 1000)
                save_chats(blob)
                BUS.publish("chat", {"id": cid, "updated": chat["updated"],
                                     "messages": chat["messages"], "name": chat["name"]},
                            origin=origin)
                return self._send_json(200, {"chat": chat})
            # legacy conversation save
            if path == "/api/conversation":
                msgs = (body or {}).get("messages", [])
                blob = load_chats()
                cid = blob.get("active")
                chat = get_chat(blob, cid)
                if chat is not None:
                    chat["messages"] = msgs
                    chat["updated"] = int(time.time() * 1000)
                    save_chats(blob)
                    BUS.publish("chat", {"id": cid, "updated": chat["updated"],
                                         "messages": msgs, "name": chat["name"]},
                                origin=origin)
                return self._send_json(200, {"ok": True})
            self._send_text(404, "not found")
        except Exception as e:
            self._send_json(500, {"ok": False, "error": str(e), "trace": traceback.format_exc()})

    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            origin = self._origin()
            if path.startswith("/api/chats/"):
                cid = path[len("/api/chats/"):]
                blob = load_chats()
                before = len(blob["chats"])
                blob["chats"] = [c for c in blob["chats"] if c.get("id") != cid]
                if len(blob["chats"]) == before:
                    return self._send_json(404, {"error": "no such chat"})
                if not blob["chats"]:
                    blob = _default_chats()
                elif blob.get("active") == cid:
                    blob["active"] = blob["chats"][0]["id"]
                save_chats(blob)
                BUS.publish("chats", self._chats_index(), origin=origin)
                return self._send_json(200, {"ok": True, "index": self._chats_index()})
            self._send_text(404, "not found")
        except Exception as e:
            self._send_json(500, {"ok": False, "error": str(e), "trace": traceback.format_exc()})

    # ── chats index helper ──

    def _chats_index(self) -> dict:
        blob = load_chats()
        return {
            "active": blob.get("active"),
            "chats": [{
                "id": c.get("id"),
                "name": c.get("name"),
                "created": c.get("created"),
                "updated": c.get("updated"),
                "size": len(c.get("messages", [])),
            } for c in blob.get("chats", [])],
        }

    # ── SSE ──

    def _sse_stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Connection", "keep-alive")
        for k, v in CORS.items():
            self.send_header(k, v)
        self.end_headers()
        q = BUS.subscribe()
        try:
            # initial hello
            hello = json.dumps({"kind": "hello", "ts": int(time.time() * 1000)})
            self.wfile.write(f"data: {hello}\n\n".encode("utf-8"))
            self.wfile.flush()
            last_ping = time.time()
            while True:
                try:
                    data = q.get(timeout=15.0)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    # heartbeat
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    if time.time() - last_ping > 60:
                        last_ping = time.time()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            BUS.unsubscribe(q)

    def _flatten_mcp_result(self, result):
        if isinstance(result, dict):
            content = result.get("content")
            if isinstance(content, list):
                parts = []
                for c in content:
                    if isinstance(c, dict):
                        if c.get("type") == "text":
                            parts.append(c.get("text", ""))
                        elif c.get("type") == "image":
                            parts.append(f"[image: {c.get('mimeType', '?')} ({len(c.get('data',''))} bytes b64)]")
                        else:
                            parts.append(json.dumps(c))
                    else:
                        parts.append(str(c))
                return "\n".join(parts)
            if "text" in result:
                return result["text"]
        if isinstance(result, str):
            return result
        return json.dumps(result, indent=2)

    # ── LLM proxy (SSE-aware) ──

    def _proxy_llm(self, body: dict):
        url = body.get("url")
        headers = body.get("headers") or {}
        payload = body.get("body") or {}
        stream = bool(body.get("stream"))
        method = (body.get("method") or "POST").upper()
        if not url:
            return self._send_json(400, {"error": "missing url"})
        try:
            data = None
            req_headers = {**headers}
            if method != "GET":
                data = json.dumps(payload).encode("utf-8")
                req_headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
            resp = urllib.request.urlopen(req, timeout=600)
            if stream:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                for k, v in CORS.items():
                    self.send_header(k, v)
                self.end_headers()
                while True:
                    chunk = resp.read(512)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        break
                return
            response_data = resp.read()
            self._hdr(resp.status, resp.headers.get("Content-Type", "application/json"), len(response_data))
            self.wfile.write(response_data)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read()
            except Exception:
                err_body = json.dumps({"error": str(e)}).encode()
            ct = e.headers.get("Content-Type", "application/json") if e.headers else "application/json"
            self._hdr(e.code, ct, len(err_body))
            self.wfile.write(err_body)
        except Exception as e:
            self._send_json(502, {"error": f"proxy: {e}", "trace": traceback.format_exc()})


# ───────────────────────────── Main ─────────────────────────────

MGR = MCPManager()

BANNER = r"""
   ┌──────────────────────────────────────────────────────────┐
   │                                                          │
   │           ▟█▙ ▙ ▟ ▟▘▝▙ ▝█▘ ▟▀▙ ▟▀▙ █                      │
   │           █▘  █▙█ █▘▝█  █  █▙▟ █▙▟ █                      │
   │           █▖  █▘█ █▘▝█  █  ▝▀█ ▝▀█ █                      │
   │           ▝█▘ █ █ ▝█▟▘  █  ▝▀▘ ▝▀▘ ▀                      │
   │                                                          │
   │    CHATSGI / TERMINAL    Section 9    rev. 2.0           │
   │    Stand Alone Complex · MCP Bridge · LAN-Sync           │
   │                                                          │
   └──────────────────────────────────────────────────────────┘
"""

def _detect_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"

def main():
    ap = argparse.ArgumentParser(description="CHATSGI / TERMINAL backend")
    ap.add_argument("--host", default=None, help="bind address (overrides settings.networkVisible)")
    ap.add_argument("--port", type=int, default=8765, help="bind port")
    ap.add_argument("--no-mcp", action="store_true", help="don't auto-start MCP servers from tools.json")
    args = ap.parse_args()

    print(BANNER)

    if not SETTINGS_PATH.exists():
        save_settings(dict(DEFAULT_SETTINGS))
        print(f"   ▸ wrote default {SETTINGS_PATH.name}")
    if not TOOLS_PATH.exists():
        save_tools_config({"mcpServers": {}})
        print(f"   ▸ wrote empty   {TOOLS_PATH.name}")
    load_chats()  # bootstrap

    settings = load_settings()
    host = args.host if args.host is not None else ("0.0.0.0" if settings.get("networkVisible") else "127.0.0.1")
    port = args.port

    print(f"   host         {host}")
    print(f"   port         {port}")
    print(f"   network      {'LAN VISIBLE' if host == '0.0.0.0' else 'LOCAL ONLY'}")
    print(f"   settings     {SETTINGS_PATH.name}")
    print(f"   chats        {CHATS_PATH.name}")
    print(f"   tools        {TOOLS_PATH.name}")

    if not args.no_mcp:
        cfg = load_tools_config()
        n = len((cfg or {}).get("mcpServers", {}))
        print(f"   ▸ spawning {n} MCP server(s)…")
        try:
            MGR.load(cfg)
        except Exception as e:
            print(f"   ! mcp boot failure: {e}")

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"\n   → http://{host if host != '0.0.0.0' else '127.0.0.1'}:{port}")
    if host == "0.0.0.0":
        print(f"   → http://{_detect_lan_ip()}:{port}  (LAN)")
    print()

    def shutdown(*_):
        print("\n   ▸ stopping MCP servers…")
        MGR.stop_all()
        try:
            server.shutdown()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        server.serve_forever()
    finally:
        MGR.stop_all()


if __name__ == "__main__":
    main()

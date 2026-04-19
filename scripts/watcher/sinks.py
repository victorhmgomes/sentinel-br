"""
sinks.py — destinos de alertas: stdout colorido + JSONL rotativo + Telegram.
"""
from __future__ import annotations
import json, os, sys
import urllib.request, urllib.parse, urllib.error
from pathlib import Path
from datetime import datetime, timezone

# cores ANSI básicas (sem dep externa)
_COLORS = {
    "reset":   "\033[0m",
    "bold":    "\033[1m",
    "dim":     "\033[2m",
    "red":     "\033[31m",
    "green":   "\033[32m",
    "yellow":  "\033[33m",
    "blue":    "\033[34m",
    "magenta": "\033[35m",
    "cyan":    "\033[36m",
    "bg_red":  "\033[41m",
    "bg_yel":  "\033[43m",
}

_SEV_COLOR = {
    "critical": _COLORS["bg_red"] + _COLORS["bold"],
    "high":     _COLORS["red"]    + _COLORS["bold"],
    "medium":   _COLORS["yellow"] + _COLORS["bold"],
    "info":     _COLORS["blue"],
}

_SEV_ICON = {"critical": "🔴", "high": "🟠", "medium": "🟡", "info": "🔵"}


def _color(s: str, col: str) -> str:
    if not sys.stdout.isatty():
        return s
    return f"{_COLORS.get(col,'')}{s}{_COLORS['reset']}"


# ---------- stdout ----------
class StdoutSink:
    def __init__(self, min_severity: str = "medium"):
        order = ["info", "medium", "high", "critical"]
        self.min_idx = order.index(min_severity)
        self.order = order

    def emit(self, alert):
        a = alert.to_dict() if hasattr(alert, "to_dict") else alert
        sev = a["severity"]
        if self.order.index(sev) < self.min_idx:
            return
        ts = a["ts"][11:19]
        head_raw = f" {sev.upper():8s} "
        head = head_raw
        if sys.stdout.isatty():
            head = f"{_SEV_COLOR.get(sev,'')}{head_raw}{_COLORS['reset']}"
        venue = _color(a["venue"], "cyan")
        asset = _color(a["asset"], "magenta")
        rule  = _color(a["rule"],  "dim")
        icon = _SEV_ICON.get(sev, "·")
        line = f"{icon} {ts}  {head}  {venue:20s}  {asset:10s}  {rule}"
        print(line)
        print(f"   ↳ {a['narrative']}")
        # contexto compacto
        ctx = a.get("context") or {}
        if ctx:
            kv = "  ".join(f"{k}={_fmt(v)}" for k, v in list(ctx.items())[:5])
            print(_color(f"   · {kv}", "dim"))
        print()


def _fmt(v):
    if isinstance(v, float):
        return f"{v:.4f}"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}={_fmt(val)}" for k, val in list(v.items())[:3]) + "}"
    return str(v)


# ---------- JSONL rotativo ----------
class JsonlSink:
    """Grava 1 alerta por linha em data/alerts/YYYY-MM-DD.jsonl"""
    def __init__(self, root: str = "data/alerts"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for_today(self) -> Path:
        d = datetime.now(timezone.utc).date().isoformat()
        return self.root / f"{d}.jsonl"

    def emit(self, alert):
        a = alert.to_dict() if hasattr(alert, "to_dict") else alert
        with self._path_for_today().open("a", encoding="utf-8") as f:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")


# ---------- snapshot diário (rolling) ----------
class StateSnapshotSink:
    """Mantém arquivo data/alerts/_live.json com últimos N alertas
       (para o dashboard mostrar feed live sem precisar ler o JSONL inteiro)."""
    def __init__(self, path: str = "data/alerts/_live.json", keep: int = 200):
        self.path = Path(path); self.keep = keep
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("[]")

    def emit(self, alert):
        a = alert.to_dict() if hasattr(alert, "to_dict") else alert
        try:
            cur = json.loads(self.path.read_text() or "[]")
        except Exception:
            cur = []
        cur.append(a)
        cur = cur[-self.keep:]
        self.path.write_text(json.dumps(cur, ensure_ascii=False))


# ---------- Telegram ----------
class TelegramSink:
    """Envia alerta para um chat do Telegram via Bot API.

    Credenciais em env:
      TELEGRAM_BOT_TOKEN  (do @BotFather)
      TELEGRAM_CHAT_ID    (do usuário ou grupo)

    Se alguma env faltar, o sink fica inativo (emit() no-op) em vez de crashar
    — útil pra rodar localmente sem bot configurado.
    """
    API = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self,
                 token: str | None = None,
                 chat_id: str | None = None,
                 min_severity: str = "medium",
                 timeout: int = 8):
        self.token   = token   or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self.timeout = timeout
        order = ["info", "medium", "high", "critical"]
        self.min_idx = order.index(min_severity)
        self.order = order
        if not self.enabled:
            print("[TelegramSink] env TELEGRAM_BOT_TOKEN/CHAT_ID ausente — sink desativado", file=sys.stderr)

    def _format(self, a: dict) -> str:
        sev = a.get("severity", "info")
        icon = _SEV_ICON.get(sev, "·")
        ts = a.get("ts", "")[:19].replace("T", " ")
        venue = a.get("venue", "?")
        asset = a.get("asset", "?")
        rule  = a.get("rule",  "?")
        narr  = a.get("narrative", "")
        ctx   = a.get("context") or {}
        ctx_line = ""
        if ctx:
            kv = "  ".join(f"<code>{k}={_fmt(v)}</code>" for k, v in list(ctx.items())[:4])
            ctx_line = f"\n<i>{kv}</i>"
        return (
            f"{icon} <b>{sev.upper()}</b> · {venue} · {asset}\n"
            f"<code>{rule}</code>  <i>{ts} UTC</i>\n"
            f"{narr}"
            f"{ctx_line}"
        )

    def emit(self, alert):
        if not self.enabled:
            return
        a = alert.to_dict() if hasattr(alert, "to_dict") else alert
        sev = a.get("severity", "info")
        if self.order.index(sev) < self.min_idx:
            return
        text = self._format(a)
        payload = urllib.parse.urlencode({
            "chat_id": self.chat_id,
            "text":    text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        url = self.API.format(token=self.token)
        req = urllib.request.Request(url, data=payload, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status != 200:
                    print(f"[TelegramSink] HTTP {resp.status}", file=sys.stderr)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:200]
            print(f"[TelegramSink] HTTP {e.code}: {body}", file=sys.stderr)
        except Exception as e:
            print(f"[TelegramSink] erro: {e}", file=sys.stderr)


# ---------- multi ----------
class MultiSink:
    def __init__(self, sinks: list):
        self.sinks = sinks

    def emit(self, alert):
        for s in self.sinks:
            try:
                s.emit(alert)
            except Exception as e:
                print(f"[sink {type(s).__name__}] erro: {e}", file=sys.stderr)


def default_sink(min_severity: str = "medium", root: str = "data/alerts",
                 telegram: bool = True, telegram_min_severity: str = "medium") -> MultiSink:
    """Sinks padrão. Se telegram=True e as envs estiverem setadas, inclui TelegramSink."""
    sinks: list = [
        StdoutSink(min_severity=min_severity),
        JsonlSink(root=root),
        StateSnapshotSink(path=f"{root}/_live.json"),
    ]
    if telegram:
        ts = TelegramSink(min_severity=telegram_min_severity)
        if ts.enabled:
            sinks.append(ts)
    return MultiSink(sinks)

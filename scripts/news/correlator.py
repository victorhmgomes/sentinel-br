"""
correlator.py — cruza notícias classificadas com alertas do watcher.

Lógica:
  - Para cada news_item com severidade >= medium, procura alertas do watcher
    numa janela [news.ts - back_hours, news.ts + forward_hours].
  - Retorna news enriquecidas com lista de alertas correlacionados.
  - Também calcula "score_corroboracao" = min(5, len(alerts) + sev_bonus).
"""
from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


def _parse_iso(s: str) -> datetime:
    """Robusto pra timestamps com/sem timezone."""
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def load_watcher_alerts(root: Path) -> list[dict]:
    """Lê todos os data/alerts/*.jsonl mais _live.json (último estado)."""
    alerts: list[dict] = []
    for p in sorted(root.glob("*.jsonl")):
        try:
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line: continue
                try: alerts.append(json.loads(line))
                except Exception: pass
        except Exception:
            pass
    live = root / "_live.json"
    if live.exists():
        try:
            cur = json.loads(live.read_text(encoding="utf-8") or "[]")
            # dedupe por chave composta (ts + rule + venue + asset)
            seen = {(a.get("ts"), a.get("rule"), a.get("venue"), a.get("asset")) for a in alerts}
            for a in cur:
                k = (a.get("ts"), a.get("rule"), a.get("venue"), a.get("asset"))
                if k not in seen: alerts.append(a)
        except Exception:
            pass
    return alerts


def correlate(
    news_items: Iterable[dict],
    alerts: Iterable[dict],
    back_hours: int = 6,
    forward_hours: int = 72,
) -> list[dict]:
    """Anota cada news_item com lista de alertas na janela e score_corroboracao."""
    alerts = list(alerts)
    out: list[dict] = []
    for n in news_items:
        t_news = _parse_iso(n["ts_iso"])
        lo = t_news - timedelta(hours=back_hours)
        hi = t_news + timedelta(hours=forward_hours)
        matched = []
        for a in alerts:
            ts = a.get("ts")
            if not ts: continue
            t_a = _parse_iso(ts)
            if lo <= t_a <= hi:
                matched.append({
                    "ts": a.get("ts"), "rule": a.get("rule"),
                    "severity": a.get("severity"), "venue": a.get("venue"),
                    "asset": a.get("asset"), "narrative": a.get("narrative"),
                })
        # score de corroboração: 0-5
        sev_weight = {"critical": 3, "high": 2, "medium": 1, "low": 0, "noise": 0}
        news_sev = n.get("_severity", "noise")
        bonus = sev_weight.get(news_sev, 0)
        score = min(5, len(matched) + bonus)
        item = dict(n)
        item["_alerts"] = matched
        item["_n_alerts"] = len(matched)
        item["_corroboration_score"] = score
        out.append(item)
    return out


def summarize(corr_items: list[dict]) -> dict:
    by_sev = {"critical": 0, "high": 0, "medium": 0, "low": 0, "noise": 0}
    for c in corr_items:
        by_sev[c.get("_severity","noise")] = by_sev.get(c.get("_severity","noise"),0) + 1
    with_alerts = sum(1 for c in corr_items if c.get("_n_alerts",0) > 0)
    return {
        "n_items": len(corr_items),
        "by_severity": by_sev,
        "with_watcher_corroboration": with_alerts,
        "top_sources": _top_sources(corr_items, 6),
    }


def _top_sources(items, k=6):
    cnt: dict[str,int] = {}
    for it in items:
        s = it.get("source","")
        cnt[s] = cnt.get(s,0) + 1
    return sorted(cnt.items(), key=lambda x: x[1], reverse=True)[:k]

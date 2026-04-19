"""
fetcher.py — puxa RSS em paralelo, normaliza pra dict uniforme, dedupe por link.

Saída: lista de itens
  {source, bucket, title, link, summary, ts_iso, hash}
"""
from __future__ import annotations
import gzip, hashlib, io, re, sys, urllib.request, urllib.error, zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from feeds import FEEDS

UA = {
    "User-Agent": "sentinel-br/1.0 (+news corroboration; research)",
    "Accept-Encoding": "gzip, deflate",
}
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE  = re.compile(r"\s+")


def _strip_html(s: str) -> str:
    if not s: return ""
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", s)).strip()


def _iso_from_rss(date_str: str) -> str:
    if not date_str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get(url: str, timeout: float = 10.0) -> bytes | None:
    """Retorna bytes crus (descompactados se gzip/deflate). ET parseia o encoding do XML."""
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            enc = (r.headers.get("Content-Encoding") or "").lower()
            # fallback: se o body começa com magic bytes gzip, descompacta
            if enc == "gzip" or data[:3] == b"\x1f\x8b\x08":
                data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
            elif enc == "deflate":
                try:    data = zlib.decompress(data)
                except zlib.error: data = zlib.decompress(data, -zlib.MAX_WBITS)
            return data
    except Exception as e:
        print(f"[fetcher] WARN {url}: {e}", file=sys.stderr)
        return None


def _parse_rss(xml_data: bytes, source: str, bucket: str) -> list[dict]:
    """Parser RSS 2.0 / Atom minimalista (sem deps). Recebe bytes."""
    out = []
    if not xml_data or not xml_data.strip():
        print(f"[fetcher] vazio em {source}", file=sys.stderr)
        return out
    # strip BOM e whitespace inicial (alguns feeds servem isso)
    xml_data = xml_data.lstrip(b"\xef\xbb\xbf").lstrip()
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"[fetcher] XML inválido em {source}: {e} (primeiros bytes: {xml_data[:60]!r})", file=sys.stderr)
        return out

    # RSS 2.0
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link  = (item.findtext("link")  or "").strip()
        desc  = _strip_html(item.findtext("description") or "")
        pub   = item.findtext("pubDate") or item.findtext("{http://purl.org/dc/elements/1.1/}date")
        if not link and not title:
            continue
        out.append({
            "source": source, "bucket": bucket,
            "title": title, "link": link,
            "summary": desc[:600],
            "ts_iso": _iso_from_rss(pub or ""),
        })

    # Atom
    if not out:
        ns = {"a": "http://www.w3.org/2005/Atom"}
        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
            link_el = entry.find("a:link", ns)
            link = link_el.get("href") if link_el is not None else ""
            summ  = _strip_html(entry.findtext("a:summary", default="", namespaces=ns) or
                                entry.findtext("a:content", default="", namespaces=ns) or "")
            pub   = entry.findtext("a:updated", default="", namespaces=ns) or \
                    entry.findtext("a:published", default="", namespaces=ns)
            out.append({
                "source": source, "bucket": bucket,
                "title": title, "link": link,
                "summary": summ[:600],
                "ts_iso": _iso_from_rss(pub or ""),
            })
    return out


def _hashkey(item: dict) -> str:
    basis = (item.get("link") or item.get("title") or "").strip().lower()
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def fetch_all(feeds: Iterable[dict] | None = None) -> list[dict]:
    """Puxa todos os feeds em paralelo. Dedupe por link/title."""
    feeds = list(feeds or FEEDS)
    items: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_get, f["url"]): f for f in feeds}
        for fut in as_completed(futs):
            f = futs[fut]
            xml = fut.result()
            if not xml:
                continue
            parsed = _parse_rss(xml, f["source"], f["bucket"])
            print(f"[fetcher] {f['source']:<22s} {len(parsed)} itens", file=sys.stderr)
            for it in parsed:
                it["hash"] = _hashkey(it)
                items.append(it)

    # dedupe
    seen, dedup = set(), []
    for it in items:
        if it["hash"] in seen: continue
        seen.add(it["hash"])
        dedup.append(it)

    # sort desc por ts
    dedup.sort(key=lambda x: x["ts_iso"], reverse=True)
    return dedup


if __name__ == "__main__":
    items = fetch_all()
    print(f"\nTOTAL: {len(items)} itens únicos")
    for i in items[:8]:
        print(f"  [{i['ts_iso'][:16]}] {i['source']:<18s} · {i['title'][:80]}")

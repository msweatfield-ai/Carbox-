import asyncio, re, os, datetime, json
from urllib.parse import urljoin, urlparse
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ----------------------------
# Config
# ----------------------------
BASE = "https://www.carboxautosales.com"
INV_URL = f"{BASE}/inventory/"

VIN_RE  = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

# ----------------------------
# HTML parsing helpers
# ----------------------------
def extract_specs_from_html(html: str, url_hint: str = "") -> dict:
    """Pull Year/Make/Model/VIN from DOM text + JSON-LD, with URL-based fallbacks."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # --- VIN from JSON-LD if present
    vin = None
    for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(tag.string or "")
            stack = [data]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    for _, v in cur.items():
                        if isinstance(v, (dict, list)):
                            stack.append(v)
                        elif isinstance(v, str) and VIN_RE.fullmatch(v.strip()):
                            vin = v.strip().upper()
                            break
                elif isinstance(cur, list):
                    stack.extend(cur)
                if vin:
                    break
        except Exception:
            pass
        if vin:
            break

    # --- VIN from visible text
    if not vin:
        m = VIN_RE.search(text)
        if m:
            vin = m.group(1).upper()

    # --- Year / Make / Model
    title = (soup.title.string if soup.title else "").strip()
    year = ""
    ym = YEAR_RE.search((title or "").upper())
    if ym:
        year = ym.group(0)

    make = ""
    model = ""

    # URL-based fallback for make/model (common dealer URL pattern)
    url = url_hint or ""
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        url = canonical["href"]

    if url and "/inventory/" in url:
        tail = url.split("/inventory/", 1)[-1].strip("/")
        parts = [p for p in tail.split("/") if p]
        if len(parts) >= 2:
            make = parts[0].upper()
            model = parts[1].upper()

    # SPECIFICATIONS block (if platform exposes it)
    spec_block = soup.find(string=re.compile(r"SPECIFICATIONS", re.I))
    if spec_block:
        blk = spec_block.find_parent()
        if blk:
            blk_text = blk.get_text(" ", strip=True).upper()
            ym2 = YEAR_RE.search(blk_text)
            if ym2:
                year = ym2.group(0)
            if not make:
                m_mk = re.search(r"\bMAKE\s+([A-Z0-9\-\s]+)", blk_text)
                if m_mk: make = m_mk.group(1).strip()
            if not model:
                m_md = re.search(r"\bMODEL\s+([A-Z0-9\-\s]+)", blk_text)
                if m_md: model = m_md.group(1).strip()

    return {
        "year":  year or "",
        "make":  make or "",
        "model": model or "",
        "vin":   vin or "",
        "url":   url or url_hint or ""
    }

# ----------------------------
# Listing page crawling
# ----------------------------
async def collect_vehicle_urls(playwright) -> list:
    """Return a de-duped list of vehicle detail URLs across ALL inventory pages."""
    browser = await playwright.chromium.launch()
    try:
        page = await browser.new_page()

        async def harvest_vehicle_links_on_page() -> set:
            urls = set()
            anchors = await page.locator("a[href*='/inventory/']").all()
            for a in anchors:
                href = await a.get_attribute("href")
                if not href:
                    continue
                href = urljoin(BASE, href)
                # skip inventory root
                if href.rstrip("/") in {BASE + "/inventory", BASE + "/inventory/"}:
                    continue
                urls.add(href.split("?")[0].split("#")[0])
            return urls

        async def discover_next_pages() -> list:
            """Find next/numbered pagination links on the current page."""
            found = set()
            sel = "a[rel='next'], a[aria-label*='Next' i], a:has-text('Next'), a:has-text('›'), a:has-text('»')"
            for a in await page.locator(sel).all():
                href = await a.get_attribute("href")
                if href:
                    found.add(urljoin(BASE, href))
            # numbered pages (1..N)
            for a in await page.locator("a").all():
                try:
                    text = (await a.inner_text() or "").strip()
                except Exception:
                    continue
                if text.isdigit():
                    href = await a.get_attribute("href")
                    if href:
                        found.add(urljoin(BASE, href))
            return sorted(found)

        # start with page 1
        await page.goto(INV_URL, wait_until="networkidle")
        all_vehicle_links = set()
        seen_listing_pages = set()
        to_visit = [INV_URL]

        # brute-force patterns up to 20 pages to avoid missing hidden pagination
        for i in range(2, 21):
            to_visit.append(f"{INV_URL}?page={i}")
            to_visit.append(urljoin(INV_URL, f"page/{i}/"))

        while to_visit:
            url = to_visit.pop(0)
            if url in seen_listing_pages:
                continue
            seen_listing_pages.add(url)

            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
            except Exception:
                continue

            # lazy load
            for _ in range(6):
                await page.mouse.wheel(0, 20000)
                await page.wait_for_timeout(250)

            # vehicle links
            all_vehicle_links |= await harvest_vehicle_links_on_page()

            # discover linked pagination from this page
            for nxt in await discover_next_pages():
                if nxt not in seen_listing_pages:
                    to_visit.append(nxt)

        return sorted(all_vehicle_links)
    finally:
        await browser.close()

# ----------------------------
# Scrape detail pages (DOM + JSON)
# ----------------------------
async def scrape_today() -> list:
    """Scrape vehicle pages and return dicts with year/make/model/vin/url (unique by VIN)."""
    async with async_playwright() as pw:
        urls = await collect_vehicle_urls(pw)

        rows = []
        browser = await pw.chromium.launch()
        try:
            page = await browser.new_page()

            # Capture VINs that appear only in background JSON calls
            json_vins = set()

            def looks_like_inventory_json(resp):
                try:
                    ct = (resp.headers or {}).get("content-type", "")
                except Exception:
                    ct = ""
                url = resp.url.lower()
                return ("application/json" in ct) and (
                    "/inventory" in url or "vehicle" in url or "listing" in url or "stock" in url
                )

            @page.on("response")
            async def handle_response(resp):
                if not looks_like_inventory_json(resp):
                    return
                try:
                    data = await resp.json()
                except Exception:
                    return
                stack = [data]
                while stack:
                    cur = stack.pop()
                    if isinstance(cur, dict):
                        for _, v in cur.items():
                            if isinstance(v, (dict, list)):
                                stack.append(v)
                            elif isinstance(v, str) and VIN_RE.fullmatch(v.strip()):
                                json_vins.add(v.strip().upper())
                    elif isinstance(cur, list):
                        stack.extend(cur)

            for u in urls:
                try:
                    await page.goto(u, wait_until="networkidle", timeout=60000)
                    await page.wait_for_timeout(600)  # let late JS finish
                    html = await page.content()
                    # include plain text for frameworks that hide text from HTML
                    try:
                        body_text = await page.evaluate("document.body.innerText")
                        if body_text:
                            html += "\n" + body_text
                    except Exception:
                        pass

                    specs = extract_specs_from_html(html, url_hint=u)
                    if not specs["vin"] and json_vins:
                        used = {r["vin"] for r in rows if r.get("vin")}
                        for v in json_vins:
                            if v not in used:
                                specs["vin"] = v
                                break

                    if specs["vin"]:
                        rows.append(specs)
                except Exception:
                    pass
        finally:
            await browser.close()

        # de-dupe by VIN
        uniq = {}
        for r in rows:
            v = r.get("vin", "")
            if v and v not in uniq:
                uniq[v] = r
        return list(uniq.values())

# ----------------------------
# IO / diff / rollup
# ----------------------------
def load_prev_inventory(path: str) -> pd.DataFrame:
    if Path(path).exists():
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.DataFrame(columns=["date","year","make","model","vin","url"])

def rollup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["year","make","model","count","vins"])
    return (
        df.groupby(["year","make","model"], dropna=False)["vin"]
          .agg(count="count", vins=lambda v: ", ".join(sorted(set(v))))
          .reset_index()
          .sort_values(["year","make","model"])
          .reset_index(drop=True)
    )

def main():
    today = datetime.date.today().isoformat()
    out_dir = Path("reports")
    out_dir.mkdir(exist_ok=True)

    # previous snapshot (if any)
    prev_files = sorted(out_dir.glob("inventory_*.csv"))
    prev_path  = prev_files[-1] if prev_files else None
    prev       = load_prev_inventory(prev_path) if prev_path else pd.DataFrame(columns=["date","year","make","model","vin","url"])

    # scrape
    rows = asyncio.run(scrape_today())
    df = pd.DataFrame(rows).drop_duplicates(subset=["vin"]).fillna("")
    df.insert(0, "date", today)

    # write today's snapshot
    (out_dir / f"inventory_{today}.csv").write_text(df.to_csv(index=False))

    # compute deltas
    if prev.empty:
        added = df.copy()
        removed = pd.DataFrame(columns=df.columns)
    else:
        prev_vins = set(prev["vin"])
        curr_vins = set(df["vin"])
        added   = df[df["vin"].isin(curr_vins - prev_vins)].copy()
        removed = prev[prev["vin"].isin(prev_vins - curr_vins)].copy()

    # write rollups + delta
    rollup(added).to_csv(out_dir / f"added_by_group_{today}.csv",     index=False)
    rollup(removed).to_csv(out_dir / f"removed_by_group_{today}.csv", index=False)
    pd.concat(
        [added.assign(change="added"), removed.assign(change="removed")],
        ignore_index=True
    ).to_csv(out_dir / f"delta_{today}.csv", index=False)

    print(f"Vehicle pages scraped: {len(rows)} | unique VINs: {df.shape[0]}")

if __name__ == "__main__":
    main()

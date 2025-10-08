import asyncio, re, os, datetime, json
from urllib.parse import urljoin, urlparse
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE = "https://www.carboxautosales.com"
INV_URL = f"{BASE}/inventory/"

VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

def extract_specs_from_html(html: str, url_hint: str = ""):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Try JSON-LD first (some sites include VIN/specs here)
    vin = None
    for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            data = json.loads(tag.string or "")
            # normalize to list
            items = data if isinstance(data, list) else [data]
            for it in items:
                # common places VIN appears
                for key in ("vin", "vehicleIdentificationNumber", "sku"):
                    if isinstance(it, dict) and key in it and isinstance(it[key], str) and VIN_RE.fullmatch(it[key].strip()):
                        vin = it[key].strip().upper()
                        break
                if vin:
                    break
        except Exception:
            pass
    # Fallback: brute-force from visible text
    if not vin:
        m = VIN_RE.search(text)
        if m:
            vin = m.group(1).upper()

    # Year/Make/Model best-effort
    title = (soup.title.string if soup.title else "").strip()
    year = ""
    ym = YEAR_RE.search(title.upper())
    if ym:
        year = ym.group(0)

    make = ""
    model = ""

    # URL-based guess (robust to many dealer platforms)
    url = url_hint or ""
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        url = canonical["href"]

    if url and "/inventory/" in url:
        tail = url.split("/inventory/", 1)[-1].strip("/")
        parts = [p for p in tail.split("/") if p]
        # common patterns:
        # /make/model/ID/  OR  /make/model/  OR  /ID/
        if len(parts) >= 2:
            make = parts[0].upper()
            model = parts[1].upper()

    # SPECIFICATIONS block (if present)
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
                if m_mk:
                    make = m_mk.group(1).strip()
            if not model:
                m_md = re.search(r"\bMODEL\s+([A-Z0-9\-\s]+)", blk_text)
                if m_md:
                    model = m_md.group(1).strip()

    return {
        "year": year or "",
        "make": make or "",
        "model": model or "",
        "vin": vin or "",
        "url": url or url_hint or ""
    }

async def discover_listing_pages(page):
    """Collect all listing pages (pagination / load more) and return their URLs, including the first page."""
    seen = set([INV_URL])
    queue = [INV_URL]

    async def harvest_listing_links():
        urls = set()
        anchors = await page.locator("a").all()
        for a in anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            href = urljoin(BASE, href)
            # consider anything under /inventory/ that looks like another listing page
            if "/inventory" in urlparse(href).path and href.rstrip("/") not in (BASE + "/inventory", BASE + "/inventory/"):
                # keep candidate; we'll dedupe by behavior below
                urls.add(href)
        # also grab obvious pagination hrefs that include ?page=, /page/, ?pg=
        more = set(u for u in urls if any(k in u.lower() for k in ["?page=", "/page/", "?pg=", "&page="]))
        return list(more)

    # Scroll + click common "load more" buttons a few times
    for _ in range(8):
        await page.mouse.wheel(0, 20000)
        await page.wait_for_timeout(600)
        # try obvious load-more buttons
        for label in ["load more", "show more", "more results", "next", "›", "»"]:
            btn = page.locator(f"button:has-text('{label}') , a:has-text('{label}')")
            if await btn.count() > 0:
                try:
                    await btn.first.click()
                    await page.wait_for_load_state("networkidle")
                except Exception:
                    pass

    # collect explicit pagination links on the first page
    listing_candidates = await harvest_listing_links()

    for candidate in listing_candidates:
        if candidate not in seen:
            seen.add(candidate)
            queue.append(candidate)

    # Visit each discovered listing page and try to find more (rare but safe)
    pages_to_visit = list(queue)
    for url in pages_to_visit[1:]:
        try:
            await page.goto(url, wait_until="networkidle")
            await page.mouse.wheel(0, 20000)
            await page.wait_for_timeout(600)
            more = await harvest_listing_links()
            for m in more:
                if m not in seen:
                    seen.add(m)
                    queue.append(m)
        except Exception:
            pass

    return queue  # includes INV_URL + any extra listing pages

async def harvest_vehicle_links_on_page(page):
    """Return all /inventory/... links on the current page that are likely vehicle detail pages."""
    urls = set()
    anchors = await page.locator("a[href*='/inventory/']").all()
    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue
        href = urljoin(BASE, href)
        # exclude the inventory root only
        inv_root_variants = {BASE + "/inventory", BASE + "/inventory/"}
        if href.rstrip("/") in inv_root_variants:
            continue
        # accept everything else; dedupe later
        urls.add(href.split("?")[0].split("#")[0])
    return urls

async def collect_vehicle_urls(playwright):
    browser = await playwright.chromium.launch()
    try:
        page = await browser.new_page()
        await page.goto(INV_URL, wait_until="networkidle")

        listing_pages = await discover_listing_pages(page)

        all_links = set()
        for url in listing_pages:
            try:
                await page.goto(url, wait_until="networkidle")
                # scroll to trigger lazy cards
                for _ in range(6):
                    await page.mouse.wheel(0, 20000)
                    await page.wait_for_timeout(400)
                links = await harvest_vehicle_links_on_page(page)
                all_links |= links
            except Exception:
                pass

        return sorted(all_links)
    finally:
        await browser.close()

async def scrape_today():
    async with async_playwright() as pw:
        urls = await collect_vehicle_urls(pw)

        rows = []
        browser = await pw.chromium.launch()
        try:
            page = await browser.new_page()
            for u in urls:
                try:
                    await page.goto(u, wait_until="networkidle", timeout=60000)
                    # ensure client-rendered VIN made it to the DOM
                    await page.wait_for_timeout(500)
                    html = await page.content()
                    # As an extra fallback, pull raw innerText in case some text isn't in HTML yet
                    try:
                        body_text = await page.evaluate("document.body.innerText")
                        if body_text:
                            html = html + "\n" + body_text
                    except Exception:
                        pass
                    specs = extract_specs_from_html(html, url_hint=u)
                    if specs["vin"]:
                        rows.append(specs)
                except Exception:
                    pass
        finally:
            await browser.close()

        return rows

def load_prev_inventory(path: str):
    if Path(path).exists():
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.DataFrame(columns=["date","year","make","model","vin","url"])

def main():
    today = datetime.date.today().isoformat()
    out_dir = Path("reports")
    out_dir.mkdir(exist_ok=True)

    prev_files = sorted(out_dir.glob("inventory_*.csv"))
    prev_path = prev_files[-1] if prev_files else None
    prev = load_prev_inventory(prev_path) if prev_path else pd.DataFrame(columns=["date","year","make","model","vin","url"])

    rows = asyncio.run(scrape_today())
    df = pd.DataFrame(rows).drop_duplicates(subset=["vin"]).fillna("")
    df.insert(0, "date", today)
    (out_dir / f"inventory_{today}.csv").write_text(df.to_csv(index=False))

    if prev.empty:
        added = df.copy()
        removed = pd.DataFrame(columns=df.columns)
    else:
        prev_vins = set(prev["vin"])
        curr_vins = set(df["vin"])
        added = df[df["vin"].isin(curr_vins - prev_vins)].copy()
        removed = prev[prev["vin"].isin(prev_vins - curr_vins)].copy()

    def rollup(x):
        if x.empty:
            return pd.DataFrame(columns=["year","make","model","count","vins"])
        return (
            x.groupby(["year","make","model"], dropna=False)["vin"]
             .agg(count="count", vins=lambda v: ", ".join(sorted(set(v))))
             .reset_index()
             .sort_values(["year","make","model"])
             .reset_index(drop=True)
        )

    rollup(added).to_csv(out_dir / f"added_by_group_{today}.csv", index=False)
    rollup(removed).to_csv(out_dir / f"removed_by_group_{today}.csv", index=False)

    pd.concat([added.assign(change="added"), removed.assign(change="removed")],
              ignore_index=True).to_csv(out_dir / f"delta_{today}.csv", index=False)

    print(f"Vehicle pages found: {len(rows)} (unique VINs: {df.shape[0]})")

if __name__ == "__main__":
    main()

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

VIN_RE   = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
YEAR_RE  = re.compile(r"\b(19|20)\d{2}\b")
PRICE_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]{2})?")  # $12,345 or 12345

# ----------------------------
# HTML parsing helpers
# ----------------------------
def _clean_price(val: str) -> str:
    if not val: return ""
    m = PRICE_RE.search(val.replace("\u00A0"," ").replace("\u202F"," "))
    if not m: return ""
    num = m.group(1).replace(",", "")
    try:
        return str(int(float(num)))
    except Exception:
        return ""

def extract_specs_from_html(html: str, url_hint: str = "") -> dict:
    """Pull Year/Make/Model/VIN/Price from DOM text + JSON-LD, with URL-based fallbacks."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # --- VIN & price from JSON-LD if present
    vin = None
    price = ""
    for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                # price keys sometimes under offers.price
                if "offers" in cur and isinstance(cur["offers"], (dict, list)):
                    stack.append(cur["offers"])
                for k, v in cur.items():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
                    elif isinstance(v, str):
                        s = v.strip()
                        if not vin and VIN_RE.fullmatch(s):
                            vin = s.upper()
                        if not price:
                            p = _clean_price(s)
                            if p: price = p
            elif isinstance(cur, list):
                stack.extend(cur)

    # --- Fallback: VIN/price from visible text
    if not vin:
        m = VIN_RE.search(text)
        if m:
            vin = m.group(1).upper()
    if not price:
        price = _clean_price(text)

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
        "price": price or "",
        "url":   url or url_hint or ""
    }

# ----------------------------
# Listing page crawling (handles 12-per-page pagination)
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
                if href.rstrip("/") in {BASE + "/inventory", BASE + "/inventory/"}:
                    continue
                urls.add(href.split("?")[0].split("#")[0])
            return urls

        async def discover_next_pages() -> list:
            found = set()
            sel = "a[rel='next'], a[aria-label*='Next' i], a:has-text('Next'), a:has-text('›'), a:has-text('»')"
            for a in await page.locator(sel).all():
                href = await a.get_attribute("href")
                if href:
                    found.add(urljoin(BASE, href))
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

        # brute-force patterns up to 20 pages
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

            for _ in range(6):
                await page.mouse.wheel(0, 20000)
                await page.wait_for_timeout(250)

            all_vehicle_links |= await harvest_vehicle_links_on_page()

            for nxt in await discover_next_pages():
                if nxt not in seen_listing_pages:
                    to_visit.append(nxt)

        return sorted(all_vehicle_links)
    finally:
        await browser.close()

# ----------------------------
# Scrape detail pages (DOM + JSON; also collects price from JSON)
# ----------------------------
async def scrape_today() -> list:
    async with async_playwright() as pw:
        urls = await collect_vehicle_urls(pw)

        rows = []
        browser = await pw.chromium.launch()
        try:
            page = await browser.new_page()

            json_vins   = set()
            json_prices = dict()  # map VIN -> price seen in JSON

            async def handle_response(resp):
                try:
                    ct = (resp.headers or {}).get("content-type", "")
                except Exception:
                    ct = ""
                url = resp.url.lower()
                if ("application/json" not in ct) or not any(k in url for k in ["/inventory", "vehicle", "listing", "stock"]):
                    return
                try:
                    data = await resp.json()
                except Exception:
                    return
                stack = [data]
                while stack:
                    cur = stack.pop()
                    if isinstance(cur, dict):
                        for k, v in cur.items():
                            if isinstance(v, (dict, list)):
                                stack.append(v)
                            elif isinstance(v, str):
                                s = v.strip()
                                if VIN_RE.fullmatch(s):
                                    json_vins.add(s.upper())
                                else:
                                    p = _clean_price(s)
                                    if p and json_vins:
                                        # associate last seen VIN if we have one; best-effort
                                        for vv in list(json_vins):
                                            json_prices.setdefault(vv, p)
                    elif isinstance(cur, list):
                        stack.extend(cur)

            page.on("response", lambda r: asyncio.create_task(handle_response(r)))

            for u in urls:
                try:
                    await page.goto(u, wait_until="networkidle", timeout=60000)
                    await page.wait_for_timeout(600)

                    html = await page.content()
                    try:
                        body_text = await page.evaluate("document.body.innerText")
                        if body_text:
                            html += "\n" + body_text
                    except Exception:
                        pass

                    specs = extract_specs_from_html(html, url_hint=u)

                    # fill missing VIN/price from JSON signals
                    if not specs["vin"] and json_vins:
                        used = {r["vin"] for r in rows if r.get("vin")}
                        for v in json_vins:
                            if v not in used:
                                specs["vin"] = v
                                break
                    if specs["vin"] and not specs["price"]:
                        p = json_prices.get(specs["vin"], "")
                        if p:
                            specs["price"] = p

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
# IO / diff / rollup (now includes price changes)
# ----------------------------
def load_prev_inventory(path: str) -> pd.DataFrame:
    if Path(path).exists():
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.DataFrame(columns=["date","year","make","model","vin","price","url"])

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
    prev       = load_prev_inventory(prev_path) if prev_path else pd.DataFrame(columns=["date","year","make","model","vin","price","url"])

    # scrape
    rows = asyncio.run(scrape_today())
    df = pd.DataFrame(rows).drop_duplicates(subset=["vin"]).fillna("")
    # ensure price is numeric-like string
    if "price" not in df.columns: df["price"] = ""
    df["price"] = df["price"].astype(str).str.replace(r"[^0-9]", "", regex=True)
    df.insert(0, "date", today)

    # write today's snapshot
    (out_dir / f"inventory_{today}.csv").write_text(df.to_csv(index=False))

    # compute adds/removes
    if prev.empty:
        added = df.copy()
        removed = pd.DataFrame(columns=df.columns)
    else:
        prev_vins = set(prev["vin"])
        curr_vins = set(df["vin"])
        added   = df[df["vin"].isin(curr_vins - prev_vins)].copy()
        removed = prev[prev["vin"].isin(prev_vins - curr_vins)].copy()

    # compute price changes (VIN present in both, price differs and both non-empty)
    price_changes = pd.DataFrame(columns=["date","vin","year","make","model","old_price","new_price","delta","url"])
    if not prev.empty:
        merged = pd.merge(
            prev[["vin","price","year","make","model","url"]],
            df[["vin","price","year","make","model","url"]],
            on="vin", how="inner", suffixes=("_old","_new")
        )
        def to_int(s):
            try: return int(str(s).replace(",","").strip())
            except: return None
        changed = []
        for _, r in merged.iterrows():
            old = to_int(r["price_old"]); new = to_int(r["price_new"])
            if old is not None and new is not None and old != new:
                changed.append({
                    "date": today,
                    "vin": r["vin"],
                    "year": r["year_new"] or r["year_old"],
                    "make": (r["make_new"] or r["make_old"]).upper(),
                    "model": (r["model_new"] or r["model_old"]).upper(),
                    "old_price": str(old),
                    "new_price": str(new),
                    "delta": str(new - old),
                    "url": r["url_new"] or r["url_old"]
                })
        if changed:
            price_changes = pd.DataFrame(changed)

    # write rollups + delta + price changes
    rollup(added).to_csv(out_dir / f"added_by_group_{today}.csv",     index=False)
    rollup(removed).to_csv(out_dir / f"removed_by_group_{today}.csv", index=False)
    pd.concat(
        [added.assign(change="added"), removed.assign(change="removed")],
        ignore_index=True
    ).to_csv(out_dir / f"delta_{today}.csv", index=False)
    if not price_changes.empty:
        price_changes.to_csv(out_dir / f"price_changes_{today}.csv", index=False)

    print(f"Vehicle pages scraped: {len(rows)} | unique VINs: {df.shape[0]} | price changes: {0 if price_changes.empty else len(price_changes)}")

if __name__ == "__main__":
    main()

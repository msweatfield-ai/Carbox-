import asyncio, re, os, datetime
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE = "https://www.carboxautosales.com"
INV_URL = f"{BASE}/inventory/"

VIN_RE = re.compile(r'\b([A-HJ-NPR-Z0-9]{17})\b')
YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')

def extract_specs(html, url_hint=""):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    vin = None
    m = VIN_RE.search(text)
    if m:
        vin = m.group(1)

    title = (soup.title.string if soup.title else "").strip()
    canonical = soup.find("link", rel="canonical")
    url = canonical["href"] if canonical and canonical.get("href") else (url_hint or "")

    make = model = year = None
    if url and "/inventory/" in url:
        parts = url.split("/inventory/")[-1].strip("/").split("/")
        if len(parts) >= 2:
            make = parts[0].strip().upper()
            model = parts[1].strip().upper()

    ym = YEAR_RE.search(title.upper())
    if ym:
        year = ym.group(0)

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
        "year": year or "",
        "make": (make or "").upper(),
        "model": (model or "").upper(),
        "vin": vin or "",
        "url": url or url_hint or ""
    }

async def collect_vehicle_urls(playwright):
    browser = await playwright.chromium.launch()  # FIXED
    try:
        page = await browser.new_page()
        await page.goto(INV_URL, wait_until="networkidle")

        # scroll to trigger lazy-load
        for _ in range(10):
            await page.mouse.wheel(0, 20000)
            await page.wait_for_timeout(600)

        urls = set()

        async def harvest():
            anchors = await page.locator("a").all()
            for a in anchors:
                href = await a.get_attribute("href")
                if not href:
                    continue
                if href.startswith("/"):
                    href = BASE + href
                if "/inventory/" in href:
                    tail = href.split("/inventory/")[-1].strip("/")
                    if len(tail.split("/")) >= 3:
                        urls.add(href.split("?")[0].split("#")[0])

        await harvest()

        # try a generic "next" clicker (best-effort)
        while True:
            next_btn = page.locator("a,button", has_text=re.compile(r"next|older|>", re.I)).first
            try:
                if await next_btn.count() == 0:
                    break
                disabled = await next_btn.get_attribute("disabled")
                if disabled is not None:
                    break
                await next_btn.click()
                await page.wait_for_timeout(1200)
                await harvest()
            except Exception:
                break

        return sorted(urls)
    finally:
        await browser.close()  # FIXED

async def scrape_today():
    async with async_playwright() as pw:
        urls = await collect_vehicle_urls(pw)

        rows = []
        browser = await pw.chromium.launch()  # FIXED: no "async with"
        try:
            page = await browser.new_page()
            for u in urls:
                try:
                    await page.goto(u, wait_until="domcontentloaded", timeout=45000)
                    html = await page.content()
                    specs = extract_specs(html, url_hint=u)
                    if specs["vin"]:
                        rows.append(specs)
                except Exception:
                    pass
        finally:
            await browser.close()  # FIXED

        return rows

def load_prev_inventory(path):
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
    df = pd.DataFrame(rows).fillna("")
    df.insert(0, "date", today)

    snap_path = out_dir / f"inventory_{today}.csv"
    df.to_csv(snap_path, index=False)

    if not prev.empty:
        prev_vins = set(prev["vin"])
        curr_vins = set(df["vin"])
        added_vins = curr_vins - prev_vins
        removed_vins = prev_vins - curr_vins

        added = df[df["vin"].isin(added_vins)].copy()
        removed = prev[prev["vin"].isin(removed_vins)].copy()
    else:
        added = df.copy()
        removed = pd.DataFrame(columns=df.columns)

    def rollup(x):
        if x.empty:
            return pd.DataFrame(columns=["year","make","model","count","vins"])
        r = (
            x.groupby(["year","make","model"], dropna=False)["vin"]
             .agg(count="count", vins=lambda v: ", ".join(sorted(set(v))))
             .reset_index()
        )
        return r.sort_values(["year","make","model"]).reset_index(drop=True)

    added_roll = rollup(added)
    removed_roll = rollup(removed)

    added_roll.to_csv(out_dir / f"added_by_group_{today}.csv", index=False)
    removed_roll.to_csv(out_dir / f"removed_by_group_{today}.csv", index=False)

    delta_path = out_dir / f"delta_{today}.csv"
    pd.concat([added.assign(change="added"), removed.assign(change="removed")],
              ignore_index=True).to_csv(delta_path, index=False)

    print(f"Vehicles today: {len(df)}")
    print(f"Added groups: {len(added_roll)} | Removed groups: {len(removed_roll)}")

if __name__ == "__main__":
    main()

"""
99acres scraper — hybrid HTML-embedded JSON + propertySearchListingJSON API
Works with no manual DevTools step and no Playwright.

Requires:
    pip install cloudscraper bs4 pandas tenacity pydantic
"""

from __future__ import annotations
import json, logging, pathlib, random, re, sqlite3, time
from datetime import datetime
from typing import List, Optional

import cloudscraper, pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from tenacity import retry, wait_random_exponential, stop_after_attempt

SEARCH_URL = (
    "https://www.99acres.com/search/property/buy/residential-all/trivandrum"
    "?city=138&property_type=4%2C2&preference=S&area_unit=1&res_com=R&page=1"
)

RAW_HTML_DIR = pathlib.Path(__file__).resolve().parent.parent / "raw_data" / "html"
RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DB = pathlib.Path(__file__).with_suffix(".db")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

@retry(wait=wait_random_exponential(multiplier=1, max=30),
       stop=stop_after_attempt(5))
def _get_html(sess: cloudscraper.CloudScraper, url: str) -> str:
    resp = sess.get(url, timeout=(5, 40))
    resp.raise_for_status()
    return resp.text

def _rand_sleep(a=1.5, b=3.5):
    time.sleep(random.uniform(a, b))

# ─────────────────── Data models ───────────────────
class Row(BaseModel):
    title: Optional[str]
    price: Optional[str]
    area: Optional[str]
    locality: Optional[str]
    bhk: Optional[int]
    detail_url: Optional[str]
    scraped: datetime = Field(default_factory=datetime.utcnow)

# ─────────────────── Scraper ───────────────────
class NinetyNineAcresScraper:
    def __init__(self, pages: int = 50):
        self.max_pages = pages
        self.log = logging.getLogger("scraper")
        self.sess = cloudscraper.create_scraper(browser={"custom": UA})
        self.conn = sqlite3.connect(CHECKPOINT_DB)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS visited(url TEXT PRIMARY KEY)"
        )
        self.conn.commit()

    def _seen(self, url: str) -> bool:
        (row,) = self.conn.execute(
            "SELECT COUNT(*) FROM visited WHERE url=?", (url,)
        ).fetchone()
        return row > 0

    def _mark(self, url: str):
        self.conn.execute("INSERT OR IGNORE INTO visited(url) VALUES(?)", (url,))
        self.conn.commit()

    # ── main ──
    def crawl(self) -> pd.DataFrame:
        rows: list[Row] = []

        # 1) page-1 HTML
        html = _get_html(self.sess, SEARCH_URL)
        (RAW_HTML_DIR / "page1.html").write_text(html, "utf-8")
        rows.extend(self._parse_embedded(html))

        # 2) derive API base from HTML (it’s right after `"apiEndpoint":"`)
        m = re.search(r'"apiEndpoint"\s*:\s*"([^"]+propertySearchListingJSON[^"]+)"', html)
        if not m:
            self.log.error("Could not locate API endpoint in HTML. Returning page-1 only.")
            return pd.DataFrame([r.dict() for r in rows])

        api_base = m.group(1)
        # ensure &page= present exactly once
        api_base = re.sub(r"&page=\d+", "", api_base) + "&page={page}"

        # 3) loop pages 2…N
        for p in range(2, self.max_pages + 1):
            url = api_base.format(page=p)
            self.log.info("API %s", url)
            data = self.sess.get(url, timeout=(5, 30)).json()
            listings = data.get("listings") or []
            if not listings:
                break
            rows.extend(self._normalise_api(listings))
            self._mark_page(listings)
            _rand_sleep()

        return pd.DataFrame([r.dict() for r in rows])

    # ── helpers ──
    def _parse_embedded(self, html: str) -> List[Row]:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", string=re.compile("__PRELOADED_STATE__"))
        if not script:
            self.log.warning("Embedded JSON not found")
            return []
        json_text = script.string.split("=", 1)[-1].strip().rstrip(";")
        state = json.loads(json_text)
        listings = state["searchCity"]["searchCityData"]["listings"]
        return self._normalise_api(listings)

    def _normalise_api(self, lst: list[dict]) -> List[Row]:
        rows = []
        for itm in lst:
            rows.append(
                Row(
                    title=itm.get("title"),
                    price=itm.get("price"),
                    area=itm.get("size"),
                    locality=itm.get("locality"),
                    bhk=itm.get("bhk"),
                    detail_url="https://www.99acres.com" + itm["url"]
                    if itm.get("url", "").startswith("/")
                    else itm.get("url"),
                )
            )
        return rows

    def _mark_page(self, lst: list[dict]):
        for itm in lst:
            if "url" in itm:
                full = "https://www.99acres.com" + itm["url"] if itm["url"].startswith("/") else itm["url"]
                self._mark(full)

# ─────────────── CLI helper (run via -m) ───────────────
if __name__ == "__main__":
    import argparse, pathlib, sys, pandas as pd
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=50)
    args = ap.parse_args()

    scraper = NinetyNineAcresScraper(pages=args.pages)
    df = scraper.crawl()

    out = pathlib.Path(__file__).resolve().parent.parent / "raw_data" / "99acres.parquet"
    out.parent.mkdir(exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"✅  saved {len(df)} rows → {out}")

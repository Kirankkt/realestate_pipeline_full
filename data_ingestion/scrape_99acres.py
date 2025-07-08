"""Fetch a public housing dataset as a stand-in for the original 99acres scraper."""

from __future__ import annotations
import io
from datetime import datetime
import logging
import pathlib
import pandas as pd
import requests
from pydantic import BaseModel

DATA_URL = "https://raw.githubusercontent.com/ageron/handson-ml/master/datasets/housing/housing.csv"
RAW_DIR = pathlib.Path(__file__).resolve().parent.parent / "raw_data"
RAW_DIR.mkdir(exist_ok=True)

class Row(BaseModel):
    price_min_lakhs: float
    area_min_sqft: float
    location: str
    bedrooms: float | None
    scraped: datetime = datetime.utcnow()

class PublicHousingScraper:
    """Download and normalise the California housing dataset."""

    def __init__(self, url: str = DATA_URL):
        self.url = url
        self.log = logging.getLogger("scraper")

    def crawl(self) -> pd.DataFrame:
        self.log.info("Downloading %s", self.url)
        resp = requests.get(self.url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        df = df.rename(columns={"total_rooms": "area_min_sqft", "total_bedrooms": "bedrooms"})
        df["price_min_lakhs"] = df["median_house_value"] / 1e5
        df["location"] = df["ocean_proximity"]
        df["scraped"] = datetime.utcnow()
        return df[["price_min_lakhs", "area_min_sqft", "location", "bedrooms", "scraped"]]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    scraper = PublicHousingScraper()
    df = scraper.crawl()
    out = RAW_DIR / "housing.parquet"
    df.to_parquet(out, index=False)
    print(f"✅  saved {len(df)} rows → {out}")

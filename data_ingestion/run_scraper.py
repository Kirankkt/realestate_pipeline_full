"""
CLI wrapper to fetch the public California Housing dataset.

Example:
    python -m data_ingestion.run_scraper --out ../raw_data/housing.parquet
"""

import argparse
import pathlib

from .scrape_99acres import PublicHousingScraper


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=str,
        default="../raw_data/housing.parquet",
        help="Relative output path (from this file)",
    )
    args = parser.parse_args()

    df = PublicHousingScraper().crawl()

    out_path = (pathlib.Path(__file__).resolve().parent / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"✅  Saved {len(df)} rows → {out_path}")


if __name__ == "__main__":
    main()

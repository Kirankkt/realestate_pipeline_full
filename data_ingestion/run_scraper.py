"""
CLI wrapper:  python -m data_ingestion.run_scraper --pages 2
"""

import argparse
import pathlib

from .scrape_99acres import NinetyNineAcresScraper


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=2, help="How many pages to crawl")
    parser.add_argument(
        "--out",
        type=str,
        default="..\\raw_data\\99acres.parquet",
        help="Relative output path (from this file)",
    )
    args = parser.parse_args()

    df = NinetyNineAcresScraper(pages=args.pages).crawl()

    out_path = (pathlib.Path(__file__).resolve().parent / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"✅  Saved {len(df)} rows → {out_path}")


if __name__ == "__main__":
    main()

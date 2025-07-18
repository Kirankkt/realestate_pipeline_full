from prefect import flow, task

@task(retries=3, retry_delay_seconds=60)
def ingest():
    from data_ingestion.scrape_99acres import PublicHousingScraper
    df = PublicHousingScraper().crawl()
    df.to_parquet("raw_data/housing.parquet")

@task
def clean():
    from data_processing.clean_99acres import clean as _clean
    _clean("raw_data/housing.parquet", "processed/housing_clean.parquet")

@task
def train():
    import subprocess, sys
    subprocess.run([sys.executable, "model_training/train_price_model.py"], check=True)

@flow(log_prints=True)
def nightly_flow():
    ingest()
    clean()
    train()

if __name__ == "__main__":
    nightly_flow()

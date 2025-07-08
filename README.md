# Real Estate ML Pipeline

This repository demonstrates an end-to-end workflow for building a price prediction model from 99acres data.

## Installation

Create a virtual environment (optional) and install dependencies:

```bash
pip install -r requirements.txt
```

## Scrape the Dataset

Run the scraper to collect listing data. The example below crawls the first two pages:

```bash
python -m data_ingestion.run_scraper --pages 2
```

The output file is saved under `raw_data/99acres.parquet`.

## Clean the Data

Process the raw parquet into a clean dataset:

```bash
python data_processing/clean_99acres.py
```

This creates `processed/99acres_clean.parquet`.

## Train the Model

Use the cleaned dataset to train an XGBoost model:

```bash
python model_training/train_price_model.py
```

Model and preprocessing artefacts are written to `model_registry/`.

## Launch the FastAPI Service

Serve predictions using the trained model:

```bash
uvicorn api.main:app --reload
```

A `/predict` endpoint accepts JSON payloads describing a property and returns the predicted price.

## Automated Workflow with Prefect

The `orchestration/flow.py` module defines a Prefect `nightly_flow` that runs the scraper, cleans the dataset and trains the model. Execute it manually with:

```bash
python orchestration/flow.py
```

You can deploy this flow on a schedule for continuous end-to-end updates.


import pathlib
import pandas as pd
from pydantic import BaseModel, ValidationError

class CleanRow(BaseModel):
    price_min_lakhs: float
    area_min_sqft: int
    location: str
    bedrooms: int | None
    price_per_sqft: float

def clean(path_in: str, path_out: str):
    df = pd.read_parquet(path_in)
    # --- drop obvious nulls or parse errors ---- #
    df = df[df["price_min_lakhs"].notna() & df["area_min_sqft"].notna()]
    df["price_per_sqft"] = (df["price_min_lakhs"] * 1e5) / df["area_min_sqft"]

    # enforce schema
    good = []
    for _, row in df.iterrows():
        try:
            good.append(CleanRow(**row.to_dict()).dict())
        except ValidationError:
            continue
    pathlib.Path(path_out).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(good).to_parquet(path_out, index=False)

if __name__ == "__main__":
    import pathlib
    base = pathlib.Path(__file__).resolve().parent.parent
    default_out = base / "processed" / "housing_clean.parquet"
    pathlib.Path(default_out).parent.mkdir(parents=True, exist_ok=True)
    clean(base / "raw_data" / "housing.parquet", default_out)

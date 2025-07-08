from fastapi import FastAPI
from pydantic import BaseModel
import pathlib, joblib, pandas as pd

app  = FastAPI(title="California Housing Price API")
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
model = joblib.load(BASE_DIR / "model_registry" / "housing_xgb.joblib")
fe    = joblib.load(BASE_DIR / "model_registry" / "fe_pipeline.joblib")

class Query(BaseModel):
    area_min_sqft: int
    bedrooms: int
    location: str

@app.post("/predict")
def predict(q: Query):
    X = pd.DataFrame([q.dict()])
    X_fe = fe.transform(X)
    price = float(model.predict(X_fe)[0])
    return {"pred_price_lakhs": round(price, 2)}

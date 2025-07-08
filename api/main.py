from fastapi import FastAPI
from pydantic import BaseModel
import joblib, pandas as pd

app  = FastAPI(title="Trivandrum Property Price API")
model = joblib.load("../model_registry/99acres_xgb.joblib")
fe    = joblib.load("../model_registry/fe_pipeline.joblib")

class Query(BaseModel):
    area_min_sqft: int
    bedrooms: int
    location: str
    status: str

@app.post("/predict")
def predict(q: Query):
    X = pd.DataFrame([q.dict()])
    X_fe = fe.transform(X)
    price = float(model.predict(X_fe)[0])
    return {"pred_price_lakhs": round(price, 2)}

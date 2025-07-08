import mlflow, xgboost as xgb, pandas as pd, joblib
from feature_engineering.fe_pipeline import build_fe
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

import pathlib

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
df = pd.read_parquet(BASE_DIR / "processed" / "housing_clean.parquet")
y  = df["price_min_lakhs"]
X  = df.drop(columns=["price_min_lakhs"])

fe = build_fe(df)
X_fe = fe.fit_transform(X)

X_train, X_val, y_train, y_val = train_test_split(X_fe, y, test_size=0.2, random_state=42)

model = xgb.XGBRegressor(
    n_estimators=600,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
)
with mlflow.start_run():
    model.fit(X_train, y_train)
    preds = model.predict(X_val)
    r2 = r2_score(y_val, preds)
    mlflow.log_metric("val_r2", r2)
    # save artefacts
    model_dir = BASE_DIR / "model_registry"
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_dir / "housing_xgb.joblib")
    joblib.dump(fe,    model_dir / "fe_pipeline.joblib")

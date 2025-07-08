from sklearn.pipeline import Pipeline
from sklearn.compose  import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
import pandas as pd

def build_fe(df: pd.DataFrame):
    cat_cols = ["location"]
    num_cols = ["area_min_sqft", "bedrooms"]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("impute", SimpleImputer(strategy="median")),
                ("scale",  StandardScaler())
            ]), num_cols),
            ("cat", Pipeline([
                ("impute", SimpleImputer(strategy="most_frequent")),
                ("ohe",    OneHotEncoder(handle_unknown="ignore"))
            ]), cat_cols),
        ]
    )
    return pre

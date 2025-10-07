# server.py
from fastapi import FastAPI
import duckdb
import pandas as pd

app = FastAPI()

@app.get("/parquet")
def read_parquet():
    df = pd.read_parquet("data/sample.parquet")
    return df.to_dict(orient="records")

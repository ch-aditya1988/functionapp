import logging
import os
from fastapi import FastAPI, HTTPException
from azure.functions import AsgiMiddleware
import requests

app = FastAPI()

# Environment variables for Databricks access
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

@app.get("/databricks/table")
async def get_table_data():
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}"
        }
        # Define Databricks SQL endpoint URL and SQL command
        sql_endpoint = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
        data = {
            "statement": "SELECT * FROM dev_hub.csv.dummy_table",
            "warehouse_id": "31d7360fe0cb964e"  # Replace with your SQL warehouse ID
        }

        response = requests.post(sql_endpoint, headers=headers, json=data)
        response.raise_for_status()
        results = response.json()
        return results

    except requests.exceptions.RequestException as e:
        logging.error(f"Error accessing Databricks table: {e}")
        raise HTTPException(status_code=500, detail="Databricks access error")

def main(req: func.HttpRequest, context: func.Context):
    return AsgiMiddleware(app).handle(req, context)

import logging
import os
import requests
import pandas as pd
import azure.functions as func
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

URL = "https://data.1212.mn/api/v1/mn/NSO/Economy,%20environment/Consumer%20Price%20Index/DT_NSO_0600_010V1.px"

payload = {
    "query": [
        {"code": "Суурь он", "selection": {"filter": "item", "values": ["2"]}},
        {"code": "Бүлэг", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Сар", "selection": {"filter": "all", "values": ["*"]}},
    ],
    "response": {"format": "json-stat2"}
}

def main(mytimer: func.TimerRequest) -> None:
    logging.info("CPI collector function started")

    # --- Call API ---
    resp = requests.post(URL, json=payload, timeout=30)
    resp.raise_for_status()
    js = resp.json()

    # --- JSON-stat2 handling ---
    if isinstance(js, dict) and js.get("class") == "dataset":
        data = js
    elif isinstance(js, dict) and "dataset" in js:
        data = js["dataset"]
    elif isinstance(js, list):
        data = js[0]
    else:
        raise ValueError("Unexpected JSON-stat format")

    months = data["dimension"]["Сар"]["category"]["label"]
    values = data["value"]

    rows = []
    for idx, value in enumerate(values):
        if value is not None:
            rows.append({
                "date": months[str(idx)],
                "indicator": "CPI",
                "group": "Total",
                "value": float(value),
                "base_year": "2023=100",
                "source": "NSO",
                "ingested_at": datetime.utcnow()
            })

    df = pd.DataFrame(rows).sort_values("date")

    # --- Convert to CSV ---
    csv_data = df.to_csv(index=False)

    # --- ADLS Gen2 ---
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    import os

    account_name = os.environ["STORAGE_ACCOUNT_NAME"]

    if os.environ.get("WEBSITE_INSTANCE_ID"):
        # ✅ Running in Azure → Managed Identity
        credential = DefaultAzureCredential()
    else:
        # ✅ Running locally → Azure CLI login
        credential = DefaultAzureCredential(exclude_managed_identity_credential=True)

    service = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    file_system = os.environ["FILE_SYSTEM_NAME"]
    folder = os.environ["CPI_FOLDER"]
    fs_client = service.get_file_system_client(file_system)

    file_name = f"cpi_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = f"{folder}/{file_name}"

    file_client = fs_client.get_file_client(file_path)
    file_client.create_file()
    file_client.append_data(csv_data, offset=0, length=len(csv_data))
    file_client.flush_data(len(csv_data))

    logging.info(f"Saved {len(df)} rows to {file_path}")

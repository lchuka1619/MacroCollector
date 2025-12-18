import requests
import pandas as pd
from datetime import datetime

URL = "https://data.1212.mn/api/v1/mn/NSO/Economy, environment/Consumer Price Index/DT_NSO_0600_010V1.px"

payload = {
    "query": [
        {"code": "Суурь он", "selection": {"filter": "item", "values": ["2"]}},
        {"code": "Бүлэг", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Сар", "selection": {"filter": "all", "values": ["*"]}},
    ],
    "response": {"format": "json-stat2"}
}

resp = requests.post(URL, json=payload, timeout=30)
resp.raise_for_status()

import requests
import pandas as pd
from datetime import datetime

URL = "https://data.1212.mn/api/v1/mn/NSO/Economy,%20environment/Consumer%20Price%20Index/DT_NSO_0600_010V1.px"

payload = {
    "query": [
        {"code": "Суурь он", "selection": {"filter": "item", "values": ["2"]}},
        {"code": "Бүлэг", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Сар", "selection": {"filter": "all", "values": ["*"]}},
    ],
    "response": {"format": "json-stat2"}
}

resp = requests.post(URL, json=payload, timeout=30)
resp.raise_for_status()

js = resp.json()

# ✅ Robust JSON-stat2 handling
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

print(df.head())
print(df.tail())
print(f"Rows collected: {len(df)}")

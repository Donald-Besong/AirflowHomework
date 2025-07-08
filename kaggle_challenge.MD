# 📊 Thank You for setting this enjoyable test, where I built a **Python 3.11 mini-service** 

1. **Fetching the data** 
import os
import requests
import json
from pathlib import Path
import zipfile

base_url = "https://www.kaggle.com/api/v1"
owner_slug = "datasnaek"
dataset_slug = "youtube-new"
dataset_version = "115"
csv_file_name = "GBvideos.csv"
json_file_name = "GB_category_id.json"

dataset_url = f"{base_url}/datasets/download/{owner_slug}/{dataset_slug}?datasetVersionNumber={dataset_version}"
zip_filename = "youtube-new.zip"

# ─── Load API Credentials ────────────────────────────────────────────────
creds_path = Path.home() / ".kaggle" / "kaggle.json"

if not creds_path.exists():
    raise FileNotFoundError("Missing kaggle.json file at ~/.kaggle/kaggle.json")

with open(creds_path) as f:
    creds = json.load(f)

auth = (creds["username"], creds["key"])

# ─── Download Dataset ────────────────────────────────────────────────────
print(f"Downloading dataset from {dataset_url}...")
response = requests.get(dataset_url, auth=auth, stream=True)

if response.status_code != 200:
    raise Exception(f"Download failed: {response.status_code} - {response.text[:300]}")

with open(zip_filename, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

print(f"Downloaded ZIP to: {zip_filename}")

# ─── Extract Specific Files ──────────────────────────────────────────────
with zipfile.ZipFile(zip_filename, "r") as zip_ref:
    zip_ref.extract(csv_file_name)
    zip_ref.extract(json_file_name)

print(f"Extracted: {csv_file_name} and {json_file_name}")

2. **Transforms & graphs** raw files into a useful graph output.  

Please visit my got repository: **https://github.com/Donald-Besong/AirflowHomework**

---

## ✅ Core Tasks

| Step | What we need |
|------|-----------------------------------------|
| **Dataset pull** | You will find the merged data in the **plots** folder.|
| **Transformation layer** | Se my transformation code and pipeline in **interview_graphs.py**.|
| **Graphing** | Find the plots in the **plots** folder |
| **Testing** | Find my tests in **dags/tests** |
| **Logs** | Find task logs in **logs** |
**Note**
Since I did everything in a hurry, I did only two plots and a few tests.
However, the test. However, I used mocks , wich included airflow objects like Variables
and **context.

### Tooling & CI (lightweight)
| Tool | Purpose |
|------|---------|
| **pre-commit** | I applied `black`, `isort` in pre-commit hooks|
| **pytest -q**  | option -q was not needed because the total runtime < 1 minute |



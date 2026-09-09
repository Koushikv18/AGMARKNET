# AGMARKNET — Full-Scale Big Data Pipeline

> **Ministry of Agriculture & Farmers Welfare, Govt. of India**
> Resource ID: `9ef84268-d588-465a-a308-a864a43d0070`
> Coverage: ~7,000 mandis · ~300 commodities · daily records since 2013

## Why This Project Matters

Public projects on this dataset almost universally use a 5-crop, 2-year Kaggle subset (~1.1M rows, ~100 MB).
This pipeline pulls the **full history** across all commodities and states — tens of millions of rows, genuinely multi-GB — and applies a production-grade Spark treatment:

- Farmer income transparency — track whether markets actually respect MSP
- MSP violation detection — flag modal prices below declared minimum support prices
- Price-shock analysis — drought, festival, and seasonal spikes
- Arbitrage/exploitation signals — states where the same commodity trades far above the national average on the same day

## Architecture

```
data.gov.in API
      |  (paginated, partitioned by year + state)
      v
 data/raw/year=YYYY/state=*/  <-- CSV chunks
      |
      v  hdfs_ingest.sh
 HDFS /user/agmarknet/raw/
      |
      v  process.py  (PySpark)
 HDFS /user/agmarknet/processed/  <-- Parquet, partitioned year/state
      |
      v  analyze.py  (Spark SQL)
 results/*.csv
      |
      v  visualize.py  (Plotly)
 output/*.html
      |
      v
 dashboard/index.html  <-- standalone interactive dashboard
```

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env — add your data.gov.in API key and Spark/HDFS settings
```

### 3. Acquire data
```bash
# Default: pulls last 2 years (fast, for testing)
python src/acquire.py

# Full history 2013-2025 (hours, multi-GB)
python src/acquire.py --full
```

### 4. Ingest into HDFS (optional)
```bash
bash scripts/hdfs_ingest.sh
```

### 5. Process with PySpark
```bash
python src/process.py
# Quick smoke-test on 10k rows
python src/process.py --sample
```

### 6. Run analytics
```bash
python src/analyze.py
```

### 7. Generate visualizations
```bash
python src/visualize.py
```

### 8. Open dashboard
```bash
start dashboard/index.html
```

## Fields

| Field        | Type   | Description                              |
|--------------|--------|------------------------------------------|
| state        | string | Indian state name                        |
| district     | string | District within the state                |
| market       | string | Mandi (market) name                      |
| commodity    | string | Agricultural commodity                   |
| variety      | string | Variety/grade name                       |
| grade        | string | Quality grade                            |
| arrival_date | date   | Date of price record (dd/MM/yyyy in raw) |
| min_price    | float  | Minimum price (Rs/quintal)               |
| max_price    | float  | Maximum price (Rs/quintal)               |
| modal_price  | float  | Most common transaction price            |

## License

Data: Open Government Data (OGD) Platform India — data.gov.in
Code: MIT

## Badges

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![Plotly](https://img.shields.io/badge/Plotly-5.18-purple?logo=plotly)
![License](https://img.shields.io/badge/License-MIT-green)
![Data](https://img.shields.io/badge/Data-data.gov.in-blue)

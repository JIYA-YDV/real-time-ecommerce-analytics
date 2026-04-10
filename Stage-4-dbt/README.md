@"

\# Stage 4: dbt Transformation Layer



\## Overview

Transform raw streaming data into analytics-ready models with data quality tests and lineage documentation.



\## Architecture

\\`\\`\\`

Raw Data (Postgres) 

&#x20; → Staging Models (stg\_\*) 

&#x20; → Intermediate Models (int\_\*) 

&#x20; → Marts (mart\_\*)

\\`\\`\\`



\## Quick Start



\### 1. Start Database

\\`\\`\\`powershell

docker-compose up -d

\\`\\`\\`



\### 2. Install dbt

\\`\\`\\`powershell

python -m venv venv

.\\venv\\Scripts\\Activate

pip install -r requirements.txt

\\`\\`\\`



\### 3. Install dbt Packages

\\`\\`\\`powershell

dbt deps

\\`\\`\\`



\### 4. Run dbt Models

\\`\\`\\`powershell

\# Test connection

dbt debug



\# Run all models

dbt run



\# Run tests

dbt test



\# Generate documentation

dbt docs generate

dbt docs serve

\\`\\`\\`



\## Models



\### Staging Layer

\- \*\*stg\_orders\*\*: Cleaned order events

\- \*\*stg\_customers\*\*: Customer aggregations



\### Intermediate Layer

\- \*\*int\_customer\_360\*\*: Customer 360 view with risk scoring



\### Marts Layer

\- \*\*mart\_daily\_revenue\*\*: Daily revenue metrics

\- \*\*mart\_fraud\_analysis\*\*: Fraud pattern analysis



\## Data Quality Tests

\- Schema tests (unique, not\_null)

\- Custom fraud detection accuracy test

\- Great Expectations freshness checks

\- Data volume thresholds



\## Access Services

\- \*\*Postgres\*\*: localhost:5432

\- \*\*dbt Docs\*\*: http://localhost:8080 (after `dbt docs serve`)



\## Key Files

\- `dbt\_project.yml`: Project configuration

\- `profiles.yml`: Database connections

\- `models/`: SQL transformations

\- `tests/`: Data quality tests



\## Screenshots Required

1\. Terminal showing `dbt run` success

2\. Terminal showing `dbt test` passing

3\. dbt docs lineage graph (browser)

4\. Postgres query showing mart\_daily\_revenue data



\## Next Stage

Stage 5: Airflow Orchestration

"@ | Out-File -FilePath "README.md" -Encoding utf8


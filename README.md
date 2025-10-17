# Data Platform for Large-Scale Analytics

A comprehensive data platform designed for Series A startups to handle billions of event records with AI-driven analytics capabilities.

## Architecture Overview

This platform implements a modern data stack optimized for:
- **High-volume event ingestion** (billions of rows)
- **Real-time and batch processing** capabilities
- **Scalable data warehousing** with BigQuery
- **AI/ML feature serving** for product teams
- **Cost-effective operations** for early-stage startups

## Tech Stack

- **Data Warehouse**: Google BigQuery
- **Transformation**: dbt (data build tool)
- **Orchestration**: Apache Airflow (local development)
- **Data Quality**: dbt tests + custom validations
- **CI/CD**: GitHub Actions
- **Documentation**: dbt docs + architecture diagrams

## Quick Start

### Prerequisites

- Python 3.8+
- Google Cloud SDK
- dbt CLI
- Docker (for Airflow)
- Git

### Setup

1. **Clone and install dependencies**:
   ```bash
   git clone <repo-url>
   cd data-platform
   pip install -r requirements.txt
   ```

2. **Configure Google Cloud**:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   gcloud auth application-default login
   ```

3. **Set up environment variables**:
   ```bash
   export GCP_PROJECT_ID="your-project-id"
   export DBT_DATASET="data_platform"
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
   ```

4. **Create BigQuery datasets**:
   ```bash
   bq mk --dataset ${GCP_PROJECT_ID}:raw
   bq mk --dataset ${GCP_PROJECT_ID}:staging
   bq mk --dataset ${GCP_PROJECT_ID}:curated
   bq mk --dataset ${GCP_PROJECT_ID}:mart
   bq mk --dataset ${GCP_PROJECT_ID}:analytics
   ```

5. **Set up dbt**:
   ```bash
   cd dbt_project
   dbt deps
   dbt seed
   ```

6. **Run the pipeline**:
   ```bash
   dbt run
   dbt test
   ```

7. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Project Structure

```
data-platform/
├── README.md
├── requirements.txt
├── .github/
│   └── workflows/
│       └── data-pipeline.yml
├── design-doc/
│   ├── architecture-design.md
│   └── diagrams/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/
│   │   ├── curated/
│   │   ├── mart/
│   │   └── analytics/
│   ├── tests/
│   │   ├── schema_tests.yml
│   │   └── test_data_quality.sql
│   ├── seeds/
│   └── macros/
├── airflow/
│   ├── dags/
│   │   └── data_pipeline_dag.py
│   └── docker-compose.yml
├── sql/
│   ├── schema_ddl.sql
│   └── sample_queries/
│       └── analytics_queries.sql
├── leadership/
│   └── team-strategy.md
└── scripts/
    ├── generate_sample_data.py
    └── data_quality_monitor.py
```

## Detailed Setup Instructions

### 1. Google Cloud Setup

#### Create a new project:
```bash
gcloud projects create your-project-id
gcloud config set project your-project-id
```

#### Enable required APIs:
```bash
# Core APIs (available in trial)
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com

# Optional APIs (may require billing account for trial)
# gcloud services enable pubsub.googleapis.com
# gcloud services enable run.googleapis.com
```

**Note**: If you get permission errors on trial account, you can skip the optional APIs for now. The core platform works with just BigQuery and Cloud Storage.

#### Create service account:
```bash
gcloud iam service-accounts create data-platform-sa \
    --display-name="Data Platform Service Account"

gcloud projects add-iam-policy-binding data-pipeline-project-0925 \
    --member="serviceAccount:data-platform-sa@data-pipeline-project-0925.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding data-pipeline-project-0925\
    --member="serviceAccount:data-platform-sa@data-pipeline-project-0925.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
```

#### Download service account key:
```bash
gcloud iam service-accounts keys create service-account.json \
    --iam-account=data-platform-sa@data-pipeline-project-0925.iam.gserviceaccount.com
```

### 2. BigQuery Setup

#### Create datasets:
```bash
bq mk --dataset --location=US ${GCP_PROJECT_ID}:raw
bq mk --dataset --location=US ${GCP_PROJECT_ID}:staging
bq mk --dataset --location=US ${GCP_PROJECT_ID}:curated
bq mk --dataset --location=US ${GCP_PROJECT_ID}:mart
bq mk --dataset --location=US ${GCP_PROJECT_ID}:analytics
```

#### Create raw tables:
```bash
bq query --use_legacy_sql=false < sql/schema_ddl.sql
```

### 3. dbt Setup

#### Option A: dbt Cloud CLI (Recommended)
1. **Sign up for dbt Cloud**: Go to [cloud.getdbt.com](https://cloud.getdbt.com)
2. **Create a new project** and connect to your BigQuery project
3. **Use dbt Cloud CLI**:
   ```bash
   # Install dbt Cloud CLI
   npm install -g @dbt-labs/dbt-cloud-cli
   
   # Login to dbt Cloud
   dbt-cloud login
   
   # Run commands
   dbt-cloud run
   dbt-cloud test
   ```

#### Option B: dbt Core CLI (Local)
1. **Install dbt Core**:
   ```bash
   pip install dbt-core dbt-bigquery
   ```

2. **Configure profiles.yml**:
   ```bash
   cd dbt_project
   # Edit profiles.yml with your project details (file already exists)
   ```

3. **Install dbt packages**:
   ```bash
   dbt deps
   ```

4. **Test connection**:
   ```bash
   dbt debug
   ```

#### Option C: dbt Cloud Web UI
1. **Sign up for dbt Cloud**: Go to [cloud.getdbt.com](https://cloud.getdbt.com)
2. **Connect your BigQuery project**
3. **Upload your dbt project** or connect to GitHub
4. **Run models directly in the browser**

### 4. Sample Data Generation

#### Option A: Trial-Friendly Setup (Recommended for Google Cloud Trial)
```bash
# This script creates datasets, tables, and loads sample data automatically
python scripts/trial_data_ingestion.py
```

#### Option B: Manual Setup (If you have billing enabled)
```bash
# Generate sample data
python scripts/generate_sample_data.py

# Load sample data to BigQuery
bq load --source_format=CSV --skip_leading_rows=1  data-pipeline-project-0925:raw.events C:\Users\saras\data-platform\sample_data\events.csv

bq load --source_format=CSV --skip_leading_rows=1  data-pipeline-project-0925:raw.users C:\Users\saras\data-platform\sample_data\users.csv

bq load --source_format=CSV --skip_leading_rows=1  data-pipeline-project-0925:raw.sessions C:\Users\saras\data-platform\sample_data\sessions.csv
```

### 5. Run Data Pipeline

#### Run dbt models:
```bash
cd dbt_project
dbt run --target dev
```

#### Run tests:
```bash
dbt test --target dev
```

#### Generate documentation:
```bash
dbt docs generate --target dev
dbt docs serve
```

### 6. Airflow Setup (Optional)

#### Start Airflow locally:
```bash
cd airflow
docker-compose up -d
```

#### Access Airflow UI:
- Open http://localhost:8080
- Login with admin/admin

### 7. CI/CD Setup

#### Configure GitHub Secrets:
- `GCP_PROJECT_ID`: Your Google Cloud project ID
- `DBT_DATASET`: Your dbt dataset name
- `GOOGLE_APPLICATION_CREDENTIALS`: Service account key (base64 encoded)
- `SLACK_WEBHOOK_URL`: Slack webhook for notifications (optional)

#### Enable GitHub Actions:
- Push code to GitHub
- Actions will run automatically on push/PR

## Key Features

### Data Pipeline
- **Incremental processing** with partitioning and clustering
- **Data quality validation** at every stage
- **Automated testing** and monitoring
- **Cost optimization** strategies

### Analytics Capabilities
- User behavior analytics
- Event aggregation and metrics
- ML feature engineering
- Real-time dashboards

### Operational Excellence
- **CI/CD integration** for data pipelines
- **Data lineage tracking**
- **Monitoring and alerting**
- **Documentation and standards**

## Usage Examples

### Running Analytics Queries

```sql
-- Daily active users
SELECT 
    date,
    daily_active_users,
    daily_revenue
FROM `your-project.data_platform.business_metrics`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC;

-- User engagement analysis
SELECT 
    user_id,
    engagement_score_7d,
    engagement_score_30d,
    preferred_platform,
    value_segment
FROM `your-project.data_platform.user_analytics`
WHERE date = CURRENT_DATE() - 1
ORDER BY engagement_score_7d DESC;
```

### ML Feature Engineering

```sql
-- ML features for user behavior prediction
SELECT 
    user_id,
    feature_date,
    events_last_7_days,
    events_last_30_days,
    revenue_last_7_days,
    revenue_last_30_days,
    engagement_score_7d,
    engagement_score_30d,
    platform_diversity_score
FROM `your-project.data_platform.ml_features`
WHERE feature_date = CURRENT_DATE() - 1;
```

## Monitoring and Maintenance

### Data Quality Monitoring

```bash
# Run data quality checks
python scripts/data_quality_monitor.py

# Check data freshness
python scripts/check_data_freshness.py
```

### Performance Monitoring

```bash
# Monitor query performance
python scripts/performance_monitor.py

# Check costs
python scripts/cost_monitor.py
```

## Troubleshooting

### Common Issues

1. **Authentication errors**:
   ```bash
   gcloud auth application-default login
   ```

2. **dbt connection issues**:
   ```bash
   dbt debug
   ```

3. **BigQuery permission errors**:
   - Check service account permissions
   - Verify project ID and dataset names

4. **Airflow connection issues**:
   - Check Docker containers are running
   - Verify environment variables

5. **Google Cloud Trial Limitations**:
   - **API Enablement**: Some APIs require billing account
   - **Solution**: Use trial-friendly setup script
   - **Quotas**: Trial has limited BigQuery slots
   - **Solution**: Use smaller datasets for testing

6. **Trial Account Specific Issues**:
   ```bash
   # If you get permission errors, try:
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   
   # Use the trial-friendly ingestion script:
   python scripts/trial_data_ingestion.py
   ```

### Getting Help

- Check the [Architecture Design](design-doc/architecture-design.md) for technical details
- Review the [Leadership Strategy](leadership/team-strategy.md) for team building approaches
- Open an issue for bugs or feature requests

## Contributing

This is a demonstration project showcasing data platform best practices for Series A startups. Feel free to adapt the patterns and approaches for your specific use case.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `dbt test`
5. Submit a pull request

### Code Standards

- Follow SQL style guidelines
- Add tests for new models
- Update documentation
- Ensure CI/CD passes

## License

This project is licensed under the MIT License - see the LICENSE file for details.

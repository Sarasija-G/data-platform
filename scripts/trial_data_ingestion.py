#!/usr/bin/env python3
"""
Trial-Friendly Data Ingestion Script
This script provides a simple way to ingest data directly into BigQuery
without requiring Pub/Sub or Cloud Run (which need billing accounts).
"""

import os
import json
import time
import random
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

class TrialDataIngestion:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
    def create_datasets_if_not_exists(self):
        """Create BigQuery datasets if they don't exist."""
        datasets = ['raw', 'staging', 'curated', 'mart', 'analytics']
        
        for dataset_id in datasets:
            dataset_ref = self.client.dataset(dataset_id)
            try:
                self.client.get_dataset(dataset_ref)
                print(f"Dataset {dataset_id} already exists")
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                dataset = self.client.create_dataset(dataset, timeout=30)
                print(f"Created dataset {dataset_id}")
    
    def create_raw_tables(self):
        """Create raw tables with proper schema."""
        
        # Events table
        events_schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("device_type", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("app_version", "STRING"),
            bigquery.SchemaField("metadata", "JSON"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        events_table_ref = self.client.dataset('raw').table('events')
        events_table = bigquery.Table(events_table_ref, schema=events_schema)
        events_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_timestamp"
        )
        events_table.clustering_fields = ["user_id", "event_type", "platform"]
        
        try:
            self.client.get_table(events_table_ref)
            print("Events table already exists")
        except NotFound:
            events_table = self.client.create_table(events_table)
            print(f"Created events table: {events_table.table_id}")
        
        # Users table
        users_schema = [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("subscription_tier", "STRING"),
            bigquery.SchemaField("metadata", "JSON"),
        ]
        
        users_table_ref = self.client.dataset('raw').table('users')
        users_table = bigquery.Table(users_table_ref, schema=users_schema)
        users_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at"
        )
        users_table.clustering_fields = ["country", "subscription_tier"]
        
        try:
            self.client.get_table(users_table_ref)
            print("Users table already exists")
        except NotFound:
            users_table = self.client.create_table(users_table)
            print(f"Created users table: {users_table.table_id}")
    
    def ingest_sample_data(self, num_events: int = 1000):
        """Ingest sample data directly into BigQuery."""
        
        # Generate sample events
        events_data = []
        users_data = []
        
        # Create some sample users first
        user_ids = [f"user_{i:04d}" for i in range(1, 101)]
        countries = ["US", "CA", "UK", "DE", "FR"]
        subscription_tiers = ["basic", "premium", "enterprise"]
        
        for i, user_id in enumerate(user_ids):
            user_data = {
                "user_id": user_id,
                "email": f"user{i}@example.com",
                "created_at": datetime.now() - timedelta(days=random.randint(1, 365)),
                "updated_at": datetime.now() - timedelta(days=random.randint(0, 30)),
                "first_name": f"User{i}",
                "last_name": "Example",
                "country": random.choice(countries),
                "timezone": "America/New_York",
                "subscription_tier": random.choice(subscription_tiers),
                "metadata": json.dumps({"source": "trial_demo"})
            }
            users_data.append(user_data)
        
        # Generate sample events
        event_types = ["purchase", "page_view", "signup", "login", "click", "scroll"]
        platforms = ["web", "mobile"]
        device_types = ["desktop", "mobile", "tablet"]
        
        for i in range(num_events):
            user_id = random.choice(user_ids)
            event_type = random.choice(event_types)
            event_time = datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            # Generate metadata based on event type
            metadata = {}
            if event_type == "purchase":
                metadata = {
                    "amount": round(random.uniform(10, 500), 2),
                    "currency": "USD",
                    "product_id": f"prod_{random.randint(1, 10):03d}",
                    "category": random.choice(["electronics", "clothing", "books"])
                }
            elif event_type == "page_view":
                metadata = {
                    "page": random.choice(["/home", "/products", "/checkout", "/profile"]),
                    "category": random.choice(["electronics", "clothing", "books"])
                }
            
            event_data = {
                "event_id": f"evt_{i:06d}",
                "user_id": user_id,
                "event_type": event_type,
                "event_timestamp": event_time,
                "session_id": f"sess_{random.randint(1, 50):03d}",
                "device_type": random.choice(device_types),
                "platform": random.choice(platforms),
                "app_version": "1.0.0",
                "metadata": json.dumps(metadata),
                "created_at": datetime.now()
            }
            events_data.append(event_data)
        
        # Insert users data
        if users_data:
            users_df = pd.DataFrame(users_data)
            users_table_ref = self.client.dataset('raw').table('users')
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"  # Replace existing data
            )
            job = self.client.load_table_from_dataframe(
                users_df, users_table_ref, job_config=job_config
            )
            job.result()
            print(f"Inserted {len(users_data)} users")
        
        # Insert events data
        if events_data:
            events_df = pd.DataFrame(events_data)
            events_table_ref = self.client.dataset('raw').table('events')
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"  # Replace existing data
            )
            job = self.client.load_table_from_dataframe(
                events_df, events_table_ref, job_config=job_config
            )
            job.result()
            print(f"Inserted {len(events_data)} events")
    
    def run_simple_analytics(self):
        """Run some simple analytics queries to test the setup."""
        
        queries = {
            "daily_events": """
                SELECT 
                    DATE(event_timestamp) as date,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT user_id) as unique_users
                FROM `{}.raw.events`
                GROUP BY DATE(event_timestamp)
                ORDER BY date DESC
                LIMIT 10
            """.format(self.project_id),
            
            "user_activity": """
                SELECT 
                    user_id,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT event_type) as unique_event_types
                FROM `{}.raw.events`
                GROUP BY user_id
                ORDER BY total_events DESC
                LIMIT 10
            """.format(self.project_id),
            
            "event_types": """
                SELECT 
                    event_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM `{}.raw.events`
                GROUP BY event_type
                ORDER BY count DESC
            """.format(self.project_id)
        }
        
        for query_name, query in queries.items():
            print(f"\n=== {query_name.upper()} ===")
            try:
                result = self.client.query(query).to_dataframe()
                print(result.to_string(index=False))
            except Exception as e:
                print(f"Error running {query_name}: {str(e)}")

def main():
    """Main function to set up trial data platform."""
    
    project_id = os.getenv('GCP_PROJECT_ID', 'data-pipeline-project-0925')
    
    print(f"Setting up trial data platform for project: {project_id}")
    
    # Initialize ingestion
    ingestion = TrialDataIngestion(project_id)
    
    # Create datasets
    print("\n1. Creating datasets...")
    ingestion.create_datasets_if_not_exists()
    
    # Create tables
    print("\n2. Creating tables...")
    ingestion.create_raw_tables()
    
    # Ingest sample data
    print("\n3. Ingesting sample data...")
    ingestion.ingest_sample_data(num_events=1000)
    
    # Run analytics
    print("\n4. Running sample analytics...")
    ingestion.run_simple_analytics()
    
    print("\n✅ Trial data platform setup complete!")
    print("\nNext steps:")
    print("1. Run dbt models: cd dbt_project && dbt run")
    print("2. Run dbt tests: dbt test")
    print("3. Generate docs: dbt docs generate && dbt docs serve")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Data Quality Monitor
Monitors data quality metrics and sends alerts for anomalies.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityMonitor:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_data_freshness(self, table_name: str, date_column: str, max_hours: int = 4) -> dict:
        """Check if data is fresh (within max_hours)."""
        try:
            query = f"""
                SELECT 
                    MAX({date_column}) as latest_date,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({date_column}), HOUR) as hours_behind
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            """
            
            result = self.client.query(query).to_dataframe()
            
            if result.empty:
                return {
                    'status': 'error',
                    'message': f'No data found in {table_name}',
                    'latest_date': None,
                    'hours_behind': None
                }
            
            latest_date = result['latest_date'].iloc[0]
            hours_behind = result['hours_behind'].iloc[0]
            
            if hours_behind > max_hours:
                return {
                    'status': 'warning',
                    'message': f'Data in {table_name} is {hours_behind} hours behind',
                    'latest_date': str(latest_date),
                    'hours_behind': hours_behind
                }
            else:
                return {
                    'status': 'success',
                    'message': f'Data in {table_name} is fresh',
                    'latest_date': str(latest_date),
                    'hours_behind': hours_behind
                }
                
        except Exception as e:
            logger.error(f"Error checking freshness for {table_name}: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error checking freshness: {str(e)}',
                'latest_date': None,
                'hours_behind': None
            }
    
    def check_data_quality(self, table_name: str) -> dict:
        """Check data quality metrics for a table."""
        try:
            # Get table schema
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            # Check for null values in critical columns
            null_checks = []
            for field in table.schema:
                if field.name in ['user_id', 'event_id', 'event_type', 'event_timestamp']:
                    query = f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN {field.name} IS NULL THEN 1 ELSE 0 END) as null_count,
                            ROUND(SUM(CASE WHEN {field.name} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                        WHERE DATE(_PARTITIONTIME) = CURRENT_DATE() - 1
                    """
                    
                    result = self.client.query(query).to_dataframe()
                    if not result.empty:
                        null_checks.append({
                            'column': field.name,
                            'total_rows': result['total_rows'].iloc[0],
                            'null_count': result['null_count'].iloc[0],
                            'null_percentage': result['null_percentage'].iloc[0]
                        })
            
            # Check for duplicate records
            duplicate_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT *) as unique_rows,
                    COUNT(*) - COUNT(DISTINCT *) as duplicate_count
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE DATE(_PARTITIONTIME) = CURRENT_DATE() - 1
            """
            
            duplicate_result = self.client.query(duplicate_query).to_dataframe()
            duplicate_info = {
                'total_rows': duplicate_result['total_rows'].iloc[0] if not duplicate_result.empty else 0,
                'unique_rows': duplicate_result['unique_rows'].iloc[0] if not duplicate_result.empty else 0,
                'duplicate_count': duplicate_result['duplicate_count'].iloc[0] if not duplicate_result.empty else 0
            }
            
            return {
                'status': 'success',
                'table_name': table_name,
                'null_checks': null_checks,
                'duplicate_info': duplicate_info,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking data quality for {table_name}: {str(e)}")
            return {
                'status': 'error',
                'table_name': table_name,
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_business_rules(self) -> dict:
        """Check business rule validations."""
        try:
            # Check for negative revenue
            negative_revenue_query = f"""
                SELECT COUNT(*) as negative_revenue_count
                FROM `{self.project_id}.{self.dataset_id}.user_metrics`
                WHERE total_revenue < 0
                AND date = CURRENT_DATE() - 1
            """
            
            negative_revenue_result = self.client.query(negative_revenue_query).to_dataframe()
            negative_revenue_count = negative_revenue_result['negative_revenue_count'].iloc[0] if not negative_revenue_result.empty else 0
            
            # Check for future timestamps
            future_timestamp_query = f"""
                SELECT COUNT(*) as future_timestamp_count
                FROM `{self.project_id}.{self.dataset_id}.stg_events`
                WHERE event_timestamp > CURRENT_TIMESTAMP()
                AND event_date = CURRENT_DATE() - 1
            """
            
            future_timestamp_result = self.client.query(future_timestamp_query).to_dataframe()
            future_timestamp_count = future_timestamp_result['future_timestamp_count'].iloc[0] if not future_timestamp_result.empty else 0
            
            # Check for invalid event types
            invalid_event_types_query = f"""
                SELECT COUNT(*) as invalid_event_types_count
                FROM `{self.project_id}.{self.dataset_id}.stg_events`
                WHERE event_type NOT IN ('purchase', 'page_view', 'signup', 'login', 'logout', 'click', 'scroll', 'search', 'filter', 'sort', 'error', 'exception', 'crash', 'session_start', 'session_end')
                AND event_date = CURRENT_DATE() - 1
            """
            
            invalid_event_types_result = self.client.query(invalid_event_types_query).to_dataframe()
            invalid_event_types_count = invalid_event_types_result['invalid_event_types_count'].iloc[0] if not invalid_event_types_result.empty else 0
            
            return {
                'status': 'success',
                'negative_revenue_count': negative_revenue_count,
                'future_timestamp_count': future_timestamp_count,
                'invalid_event_types_count': invalid_event_types_count,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking business rules: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def generate_quality_report(self) -> dict:
        """Generate comprehensive data quality report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'project_id': self.project_id,
            'dataset_id': self.dataset_id,
            'checks': {}
        }
        
        # Check data freshness for key tables
        tables_to_check = [
            ('stg_events', 'event_timestamp'),
            ('stg_users', 'created_at'),
            ('user_metrics', 'date'),
            ('event_metrics', 'date'),
            ('business_metrics', 'date')
        ]
        
        for table_name, date_column in tables_to_check:
            report['checks'][f'{table_name}_freshness'] = self.check_data_freshness(table_name, date_column)
        
        # Check data quality for staging tables
        staging_tables = ['stg_events', 'stg_users']
        for table_name in staging_tables:
            report['checks'][f'{table_name}_quality'] = self.check_data_quality(table_name)
        
        # Check business rules
        report['checks']['business_rules'] = self.check_business_rules()
        
        return report
    
    def send_alert(self, message: str, severity: str = 'warning'):
        """Send alert notification."""
        # In a real implementation, this would send to Slack, email, etc.
        logger.info(f"ALERT [{severity.upper()}]: {message}")
        
        # Example Slack notification
        if os.getenv('SLACK_WEBHOOK_URL'):
            import requests
            payload = {
                'text': f'🚨 Data Quality Alert: {message}',
                'channel': '#data-alerts'
            }
            requests.post(os.getenv('SLACK_WEBHOOK_URL'), json=payload)

def main():
    """Main function to run data quality monitoring."""
    project_id = os.getenv('GCP_PROJECT_ID', 'your-project-id')
    dataset_id = os.getenv('DBT_DATASET', 'data_platform')
    
    monitor = DataQualityMonitor(project_id, dataset_id)
    
    # Generate quality report
    report = monitor.generate_quality_report()
    
    # Check for issues and send alerts
    issues_found = False
    for check_name, check_result in report['checks'].items():
        if check_result['status'] in ['warning', 'error']:
            issues_found = True
            monitor.send_alert(f"{check_name}: {check_result['message']}", check_result['status'])
    
    if not issues_found:
        logger.info("All data quality checks passed successfully")
    
    # Save report to file
    with open('quality_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info("Data quality monitoring completed")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Simple Data Generator for dbt Cloud
Generates sample data and saves to CSV files that can be uploaded to BigQuery
"""

import json
import random
import pandas as pd
from datetime import datetime, timedelta
import uuid

def generate_sample_data():
    """Generate sample data for the data platform."""
    
    # Configuration
    num_users = 100
    num_events = 1000
    num_days = 30
    
    # Sample data lists
    countries = ["US", "CA", "UK", "DE", "FR", "AU", "JP", "BR"]
    subscription_tiers = ["basic", "premium", "enterprise"]
    event_types = ["purchase", "page_view", "signup", "login", "logout", "click", "scroll", "search"]
    platforms = ["web", "mobile"]
    device_types = ["desktop", "mobile", "tablet"]
    categories = ["electronics", "clothing", "books", "home", "sports", "beauty"]
    
    # Generate users
    print("Generating users...")
    users = []
    for i in range(num_users):
        user_id = f"user_{i+1:04d}"
        first_name = f"User{i+1}"
        last_name = "Example"
        email = f"user{i+1}@example.com"
        
        user = {
            "user_id": user_id,
            "email": email,
            "created_at": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat() + "Z",
            "updated_at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat() + "Z",
            "first_name": first_name,
            "last_name": last_name,
            "country": random.choice(countries),
            "timezone": "America/New_York",
            "subscription_tier": random.choice(subscription_tiers),
            "metadata": json.dumps({"source": "sample_data", "campaign": "demo"})
        }
        users.append(user)
    
    # Generate events
    print("Generating events...")
    events = []
    user_ids = [user["user_id"] for user in users]
    
    for i in range(num_events):
        user_id = random.choice(user_ids)
        event_type = random.choice(event_types)
        event_time = datetime.now() - timedelta(
            days=random.randint(0, num_days),
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
                "category": random.choice(categories)
            }
        elif event_type == "page_view":
            metadata = {
                "page": random.choice(["/home", "/products", "/checkout", "/profile", "/search"]),
                "category": random.choice(categories)
            }
        elif event_type == "signup":
            metadata = {
                "source": random.choice(["google_ads", "facebook_ads", "organic", "referral"]),
                "campaign": random.choice(["summer_sale", "winter_promo", "new_user"])
            }
        
        event = {
            "event_id": f"evt_{i+1:06d}",
            "user_id": user_id,
            "event_type": event_type,
            "event_timestamp": event_time.isoformat() + "Z",
            "session_id": f"sess_{random.randint(1, 50):03d}",
            "device_type": random.choice(device_types),
            "platform": random.choice(platforms),
            "app_version": "1.0.0",
            "metadata": json.dumps(metadata),
            "created_at": datetime.now().isoformat() + "Z"
        }
        events.append(event)
    
    # Generate sessions
    print("Generating sessions...")
    sessions = {}
    
    for event in events:
        session_id = event["session_id"]
        user_id = event["user_id"]
        event_time = datetime.fromisoformat(event["event_timestamp"].replace("Z", ""))
        
        if session_id not in sessions:
            sessions[session_id] = {
                "session_id": session_id,
                "user_id": user_id,
                "started_at": event_time.isoformat() + "Z",
                "ended_at": None,
                "duration_seconds": None,
                "page_views": 0,
                "events_count": 0,
                "device_type": event["device_type"],
                "platform": event["platform"],
                "country": None,
                "metadata": json.dumps({})
            }
        
        sessions[session_id]["events_count"] += 1
        if event["event_type"] == "page_view":
            sessions[session_id]["page_views"] += 1
        
        # Update end time
        if sessions[session_id]["ended_at"] is None or event_time > datetime.fromisoformat(sessions[session_id]["ended_at"].replace("Z", "")):
            sessions[session_id]["ended_at"] = event_time.isoformat() + "Z"
    
    # Calculate durations
    for session in sessions.values():
        if session["ended_at"]:
            start = datetime.fromisoformat(session["started_at"].replace("Z", ""))
            end = datetime.fromisoformat(session["ended_at"].replace("Z", ""))
            session["duration_seconds"] = int((end - start).total_seconds())
    
    # Save to CSV files
    print("Saving to CSV files...")
    
    # Create sample_data directory
    import os
    os.makedirs("sample_data", exist_ok=True)
    
    # Save users
    users_df = pd.DataFrame(users)
    users_df.to_csv("sample_data/users.csv", index=False)
    print(f"Saved {len(users)} users to sample_data/users.csv")
    
    # Save events
    events_df = pd.DataFrame(events)
    events_df.to_csv("sample_data/events.csv", index=False)
    print(f"Saved {len(events)} events to sample_data/events.csv")
    
    # Save sessions
    sessions_df = pd.DataFrame(list(sessions.values()))
    sessions_df.to_csv("sample_data/sessions.csv", index=False)
    print(f"Saved {len(sessions)} sessions to sample_data/sessions.csv")
    
    print("\n✅ Sample data generation complete!")
    print("\nNext steps:")
    print("1. Upload these CSV files to BigQuery manually or use dbt Cloud")
    print("2. Or use the SQL queries in sql/sample_queries/ to create sample data directly in BigQuery")

if __name__ == "__main__":
    generate_sample_data()

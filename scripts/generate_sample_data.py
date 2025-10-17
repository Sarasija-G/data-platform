#!/usr/bin/env python3
"""
Sample Data Generator for Data Platform
Generates realistic sample data for testing and demonstration purposes.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd

# Configuration
NUM_USERS = 1000
NUM_DAYS = 30
EVENTS_PER_USER_PER_DAY = (1, 20)  # Min, Max events per user per day
PRODUCTS = [
    "prod_001", "prod_002", "prod_003", "prod_004", "prod_005",
    "prod_006", "prod_007", "prod_008", "prod_009", "prod_010"
]
CATEGORIES = ["electronics", "clothing", "books", "home", "sports", "beauty", "toys", "food"]
EVENT_TYPES = ["purchase", "page_view", "signup", "login", "logout", "click", "scroll", "search"]
PLATFORMS = ["web", "mobile"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "CA", "UK", "DE", "FR", "AU", "JP", "BR"]
SUBSCRIPTION_TIERS = ["basic", "premium", "enterprise"]

def generate_user_id() -> str:
    """Generate a unique user ID."""
    return f"user_{uuid.uuid4().hex[:8]}"

def generate_event_id() -> str:
    """Generate a unique event ID."""
    return f"evt_{uuid.uuid4().hex[:8]}"

def generate_session_id() -> str:
    """Generate a unique session ID."""
    return f"sess_{uuid.uuid4().hex[:8]}"

def generate_email(first_name: str, last_name: str) -> str:
    """Generate a realistic email address."""
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

def generate_user_data() -> List[Dict[str, Any]]:
    """Generate sample user data."""
    users = []
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    
    for i in range(NUM_USERS):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        
        user = {
            "user_id": generate_user_id(),
            "email": generate_email(first_name, last_name),
            "created_at": created_at.isoformat() + "Z",
            "updated_at": (created_at + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
            "first_name": first_name,
            "last_name": last_name,
            "country": random.choice(COUNTRIES),
            "timezone": random.choice(["America/New_York", "America/Los_Angeles", "Europe/London", "Europe/Berlin", "Asia/Tokyo"]),
            "subscription_tier": random.choices(SUBSCRIPTION_TIERS, weights=[0.6, 0.3, 0.1])[0],
            "metadata": json.dumps({
                "source": random.choice(["organic", "google_ads", "facebook_ads", "referral", "email"]),
                "campaign": random.choice(["summer_sale", "winter_promo", "new_user", "retention", "upsell"]),
                "referral_code": f"REF{random.randint(1000, 9999)}" if random.random() < 0.2 else None
            })
        }
        users.append(user)
    
    return users

def generate_event_data(users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate sample event data."""
    events = []
    start_date = datetime.now() - timedelta(days=NUM_DAYS)
    
    for user in users:
        user_id = user["user_id"]
        user_created_at = datetime.fromisoformat(user["created_at"].replace("Z", ""))
        
        # Generate events for each day
        for day_offset in range(NUM_DAYS):
            event_date = start_date + timedelta(days=day_offset)
            
            # Skip days before user was created
            if event_date.date() < user_created_at.date():
                continue
            
            # Determine number of events for this user on this day
            num_events = random.randint(*EVENTS_PER_USER_PER_DAY)
            
            # Generate session for this day
            session_id = generate_session_id()
            session_start = event_date + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59))
            
            for event_num in range(num_events):
                event_time = session_start + timedelta(minutes=random.randint(0, 120))
                event_type = random.choices(EVENT_TYPES, weights=[0.1, 0.4, 0.05, 0.1, 0.05, 0.15, 0.1, 0.05])[0]
                
                # Generate metadata based on event type
                metadata = {}
                if event_type == "purchase":
                    metadata = {
                        "amount": round(random.uniform(10, 500), 2),
                        "currency": random.choice(["USD", "EUR", "GBP", "CAD"]),
                        "product_id": random.choice(PRODUCTS),
                        "category": random.choice(CATEGORIES)
                    }
                elif event_type == "page_view":
                    metadata = {
                        "page": random.choice(["/home", "/products", "/checkout", "/profile", "/search"]),
                        "category": random.choice(CATEGORIES)
                    }
                elif event_type == "signup":
                    metadata = {
                        "source": random.choice(["google_ads", "facebook_ads", "organic", "referral"]),
                        "campaign": random.choice(["summer_sale", "winter_promo", "new_user"])
                    }
                elif event_type in ["click", "scroll"]:
                    metadata = {
                        "element": random.choice(["button", "link", "image", "text"]),
                        "page": random.choice(["/home", "/products", "/checkout", "/profile"])
                    }
                elif event_type == "search":
                    metadata = {
                        "query": random.choice(["laptop", "shirt", "book", "phone", "shoes"]),
                        "results_count": random.randint(0, 100)
                    }
                
                event = {
                    "event_id": generate_event_id(),
                    "user_id": user_id,
                    "event_type": event_type,
                    "event_timestamp": event_time.isoformat() + "Z",
                    "session_id": session_id,
                    "device_type": random.choice(DEVICE_TYPES),
                    "platform": random.choice(PLATFORMS),
                    "app_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                    "metadata": json.dumps(metadata),
                    "created_at": datetime.now().isoformat() + "Z"
                }
                events.append(event)
    
    return events

def generate_session_data(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate session data from events."""
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
    
    return list(sessions.values())

def save_to_csv(data: List[Dict[str, Any]], filename: str):
    """Save data to CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Saved {len(data)} records to {filename}")

def save_to_json(data: List[Dict[str, Any]], filename: str):
    """Save data to JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {filename}")

def main():
    """Generate and save sample data."""
    print("Generating sample data...")
    
    # Generate users
    print("Generating users...")
    users = generate_user_data()
    save_to_csv(users, "data-platform/sample_data/users.csv")
    save_to_json(users, "data-platform/sample_data/users.json")
    
    # Generate events
    print("Generating events...")
    events = generate_event_data(users)
    save_to_csv(events, "data-platform/sample_data/events.csv")
    save_to_json(events, "data-platform/sample_data/events.json")
    
    # Generate sessions
    print("Generating sessions...")
    sessions = generate_session_data(events)
    save_to_csv(sessions, "data-platform/sample_data/sessions.csv")
    save_to_json(sessions, "data-platform/sample_data/sessions.json")
    
    print(f"\nSample data generation complete!")
    print(f"Generated {len(users)} users")
    print(f"Generated {len(events)} events")
    print(f"Generated {len(sessions)} sessions")
    print(f"Data saved to data-platform/sample_data/")

if __name__ == "__main__":
    main()



#!/usr/bin/env python3
"""
Generate sample media analytics data locally
Files will be created in ./sample_data/ folder for manual S3 upload
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Create output directory
os.makedirs('sample_data/raw', exist_ok=True)

print("Generating sample media analytics data...")

# Configuration
NUM_DAYS = 30
NUM_USERS = 1000
NUM_CONTENT = 500
START_DATE = datetime.now() - timedelta(days=NUM_DAYS)

# Generate viewership data
print("  - Generating viewership events...")
viewership_data = []
for day in range(NUM_DAYS):
    date = START_DATE + timedelta(days=day)
    num_events = np.random.randint(5000, 15000)
    
    for _ in range(num_events):
        viewership_data.append({
            'event_id': f"view_{date.strftime('%Y%m%d')}_{_}",
            'user_id': f"user_{np.random.randint(1, NUM_USERS)}",
            'content_id': f"content_{np.random.randint(1, NUM_CONTENT)}",
            'event_timestamp': date + timedelta(
                hours=np.random.randint(0, 24),
                minutes=np.random.randint(0, 60),
                seconds=np.random.randint(0, 60)
            ),
            'watch_duration_seconds': np.random.randint(60, 7200),
            'device_type': np.random.choice(['mobile', 'tv', 'web', 'tablet'], p=[0.4, 0.3, 0.2, 0.1]),
            'quality': np.random.choice(['SD', 'HD', '4K'], p=[0.2, 0.6, 0.2]),
            'buffer_count': np.random.randint(0, 5),
        })

viewership_df = pd.DataFrame(viewership_data)
viewership_df.to_parquet('sample_data/raw/viewership.parquet', index=False)
print(f"    ✓ Created {len(viewership_df)} viewership events")

# Generate revenue data
print("  - Generating revenue transactions...")
revenue_data = []
for day in range(NUM_DAYS):
    date = START_DATE + timedelta(days=day)
    num_transactions = np.random.randint(500, 2000)
    
    for _ in range(num_transactions):
        transaction_type = np.random.choice(['subscription', 'rental', 'purchase', 'ad_revenue'], 
                                           p=[0.5, 0.2, 0.1, 0.2])
        
        if transaction_type == 'subscription':
            amount = np.random.choice([9.99, 14.99, 19.99])
        elif transaction_type == 'rental':
            amount = np.random.uniform(3.99, 6.99)
        elif transaction_type == 'purchase':
            amount = np.random.uniform(9.99, 24.99)
        else:  # ad_revenue
            amount = np.random.uniform(0.01, 0.50)
        
        revenue_data.append({
            'transaction_id': f"txn_{date.strftime('%Y%m%d')}_{_}",
            'user_id': f"user_{np.random.randint(1, NUM_USERS)}",
            'transaction_timestamp': date + timedelta(
                hours=np.random.randint(0, 24),
                minutes=np.random.randint(0, 60)
            ),
            'transaction_type': transaction_type,
            'amount_usd': round(amount, 2),
            'payment_method': np.random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay']),
            'currency': 'USD',
        })

revenue_df = pd.DataFrame(revenue_data)
revenue_df.to_csv('sample_data/raw/revenue.csv', index=False)
print(f"    ✓ Created {len(revenue_df)} revenue transactions")

# Generate content metadata
print("  - Generating content metadata...")
content_data = []
genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Documentary', 'Romance']
for i in range(1, NUM_CONTENT + 1):
    content_data.append({
        'content_id': f"content_{i}",
        'title': f"Content Title {i}",
        'genre': np.random.choice(genres),
        'duration_minutes': np.random.randint(30, 180),
        'release_year': np.random.randint(2015, 2026),
        'rating': np.random.choice(['G', 'PG', 'PG-13', 'R', 'NR']),
        'content_type': np.random.choice(['movie', 'series', 'documentary', 'short']),
    })

content_df = pd.DataFrame(content_data)
content_df.to_json('sample_data/raw/content.json', orient='records', lines=True)
print(f"    ✓ Created {len(content_df)} content items")

# Generate user data
print("  - Generating user data...")
user_data = []
for i in range(1, NUM_USERS + 1):
    signup_date = START_DATE - timedelta(days=np.random.randint(1, 365))
    user_data.append({
        'user_id': f"user_{i}",
        'signup_date': signup_date,
        'country': np.random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR'], p=[0.5, 0.15, 0.15, 0.1, 0.05, 0.05]),
        'subscription_tier': np.random.choice(['free', 'basic', 'premium'], p=[0.3, 0.4, 0.3]),
        'age_group': np.random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
    })

user_df = pd.DataFrame(user_data)
user_df.to_csv('sample_data/raw/users.csv', index=False)
print(f"    ✓ Created {len(user_df)} users")

print("\n✅ Sample data generated successfully!")
print("\nFiles created in ./sample_data/raw/:")
print("  - viewership.parquet (Parquet format)")
print("  - revenue.csv (CSV format)")
print("  - content.json (JSON Lines format)")
print("  - users.csv (CSV format)")
print("\n📤 Next step: Manually upload these files to S3:")
print("   s3://qe-de-capstone-mahmed/raw/viewership/")
print("   s3://qe-de-capstone-mahmed/raw/revenue/")
print("   s3://qe-de-capstone-mahmed/raw/content/")
print("   s3://qe-de-capstone-mahmed/raw/users/")

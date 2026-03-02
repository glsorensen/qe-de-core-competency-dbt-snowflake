#!/usr/bin/env python3
"""
Generate sample media & entertainment analytics data for Snowflake pipeline testing.
Produces realistic viewership, revenue, content, user, and subscription data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import boto3
from pathlib import Path
import argparse
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
ACCOUNT_ID = "066396400174"
BUCKET_NAME = f"snowflake-media-analytics-meagan-{ACCOUNT_ID}"
START_DATE = datetime(2024, 1, 1)
NUM_DAYS = 30
NUM_USERS = 10000
NUM_CONTENT_ITEMS = 500
NUM_VIEWERSHIP_EVENTS_PER_DAY = 50000


def generate_content_catalog(num_items=NUM_CONTENT_ITEMS):
    """Generate content metadata (movies, TV shows, etc.)"""
    print(f"Generating {num_items} content items...")
    
    genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror', 'Romance', 
              'Documentary', 'Thriller', 'Animation', 'Fantasy']
    content_types = ['Movie', 'TV Series', 'Documentary', 'Short Film']
    ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17', 'TV-Y', 'TV-PG', 'TV-14', 'TV-MA']
    
    data = []
    for i in range(1, num_items + 1):
        content_type = np.random.choice(content_types)
        data.append({
            'content_id': f'CNT{i:06d}',
            'title': f'Content Title {i}',
            'content_type': content_type,
            'genre': np.random.choice(genres),
            'release_year': np.random.randint(1990, 2024),
            'duration_minutes': np.random.randint(30, 180) if content_type == 'Movie' else np.random.randint(20, 60),
            'rating': np.random.choice(ratings),
            'production_cost': np.random.randint(100000, 50000000),
            'is_original': np.random.choice([True, False], p=[0.3, 0.7]),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        })
    
    return pd.DataFrame(data)


def generate_users(num_users=NUM_USERS):
    """Generate user profiles"""
    print(f"Generating {num_users} users...")
    
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'Mexico']
    device_types = ['Mobile', 'Tablet', 'Desktop', 'Smart TV', 'Gaming Console']
    
    data = []
    for i in range(1, num_users + 1):
        signup_date = START_DATE - timedelta(days=np.random.randint(1, 730))
        data.append({
            'user_id': f'USR{i:08d}',
            'email': f'user{i}@example.com',
            'country': np.random.choice(countries),
            'preferred_language': 'en',
            'signup_date': signup_date.date().isoformat(),
            'preferred_device': np.random.choice(device_types),
            'age_group': np.random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
            'created_at': signup_date.isoformat(),
            'updated_at': datetime.now().isoformat()
        })
    
    return pd.DataFrame(data)


def generate_subscriptions(users_df):
    """Generate subscription data for users"""
    print(f"Generating subscriptions for {len(users_df)} users...")
    
    subscription_tiers = ['Basic', 'Standard', 'Premium', 'Family']
    tier_prices = {'Basic': 9.99, 'Standard': 14.99, 'Premium': 19.99, 'Family': 24.99}
    billing_cycles = ['Monthly', 'Quarterly', 'Annual']
    
    data = []
    subscription_id = 1
    
    for _, user in users_df.iterrows():
        # 80% of users have active subscriptions
        if np.random.random() < 0.8:
            tier = np.random.choice(subscription_tiers)
            start_date = datetime.fromisoformat(user['signup_date'])
            
            data.append({
                'subscription_id': f'SUB{subscription_id:08d}',
                'user_id': user['user_id'],
                'tier': tier,
                'price_per_month': tier_prices[tier],
                'billing_cycle': np.random.choice(billing_cycles),
                'start_date': start_date.date().isoformat(),
                'end_date': None,
                'status': np.random.choice(['active', 'active', 'active', 'cancelled'], p=[0.85, 0.1, 0.04, 0.01]),
                'auto_renew': np.random.choice([True, False], p=[0.9, 0.1]),
                'created_at': start_date.isoformat(),
                'updated_at': datetime.now().isoformat()
            })
            subscription_id += 1
    
    return pd.DataFrame(data)


def generate_viewership_events(date, users_df, content_df, num_events):
    """Generate viewership events for a specific date"""
    print(f"Generating {num_events} viewership events for {date.date()}...")
    
    data = []
    for _ in range(num_events):
        user = users_df.sample(1).iloc[0]
        content = content_df.sample(1).iloc[0]
        
        view_start = date + timedelta(
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
            seconds=np.random.randint(0, 60)
        )
        
        # Watch duration (some users don't finish content)
        max_duration = content['duration_minutes']
        watch_duration = min(
            np.random.exponential(max_duration * 0.7),
            max_duration
        )
        
        data.append({
            'event_id': f'EVT{date.strftime("%Y%m%d")}_{_:010d}',
            'user_id': user['user_id'],
            'content_id': content['content_id'],
            'view_start_time': view_start.isoformat(),
            'view_end_time': (view_start + timedelta(minutes=watch_duration)).isoformat(),
            'watch_duration_minutes': round(watch_duration, 2),
            'completion_percentage': round((watch_duration / max_duration) * 100, 2),
            'device_type': user['preferred_device'],
            'platform': np.random.choice(['iOS', 'Android', 'Web', 'TV', 'Console']),
            'quality': np.random.choice(['SD', 'HD', 'FHD', '4K'], p=[0.1, 0.4, 0.35, 0.15]),
            'buffering_events': np.random.poisson(0.5),
            'country': user['country'],
            'created_at': view_start.isoformat()
        })
    
    return pd.DataFrame(data)


def generate_revenue_events(date, subscriptions_df, users_df):
    """Generate revenue transactions for a specific date"""
    print(f"Generating revenue events for {date.date()}...")
    
    data = []
    transaction_id = int(date.strftime("%Y%m%d")) * 10000
    
    # Simulate daily transactions (renewals, new subscriptions)
    # Roughly 3% of active subscriptions might have a transaction on any given day
    active_subs = subscriptions_df[subscriptions_df['status'] == 'active']
    num_transactions = int(len(active_subs) * 0.03)
    
    for _ in range(num_transactions):
        sub = active_subs.sample(1).iloc[0]
        user = users_df[users_df['user_id'] == sub['user_id']].iloc[0]
        
        transaction_time = date + timedelta(
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        # Transaction types
        transaction_type = np.random.choice(
            ['subscription_renewal', 'new_subscription', 'upgrade', 'add_on'],
            p=[0.7, 0.15, 0.1, 0.05]
        )
        
        # Calculate amount based on billing cycle
        base_amount = sub['price_per_month']
        if sub['billing_cycle'] == 'Quarterly':
            amount = base_amount * 3
        elif sub['billing_cycle'] == 'Annual':
            amount = base_amount * 12 * 0.9  # 10% discount for annual
        else:
            amount = base_amount
        
        data.append({
            'transaction_id': f'TXN{transaction_id:012d}',
            'user_id': user['user_id'],
            'subscription_id': sub['subscription_id'],
            'transaction_time': transaction_time.isoformat(),
            'transaction_type': transaction_type,
            'amount': round(amount, 2),
            'currency': 'USD',
            'payment_method': np.random.choice(['Credit Card', 'PayPal', 'Apple Pay', 'Google Pay']),
            'status': np.random.choice(['completed', 'completed', 'completed', 'failed'], p=[0.95, 0.03, 0.01, 0.01]),
            'country': user['country'],
            'created_at': transaction_time.isoformat()
        })
        transaction_id += 1
    
    return pd.DataFrame(data)


def save_to_parquet(df, filepath):
    """Save DataFrame to Parquet format with compression"""
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')
    print(f"  Saved: {filepath} ({len(df)} rows)")


def upload_to_s3(local_path, s3_key, bucket_name=BUCKET_NAME, profile='default'):
    """Upload file to S3"""
    session = boto3.Session(profile_name=profile)
    s3 = session.client('s3')
    
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"  Uploaded: s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"  Error uploading {s3_key}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Generate sample media analytics data')
    parser.add_argument('--local-only', action='store_true', help='Save files locally without uploading to S3')
    parser.add_argument('--output-dir', default='./sample_data', help='Local output directory')
    parser.add_argument('--upload-to-s3', action='store_true', help='Upload files to S3')
    parser.add_argument('--profile', default='default', help='AWS profile name')
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Generating Sample Media Analytics Data")
    print("=" * 60)
    print()
    
    # Generate master data
    content_df = generate_content_catalog()
    users_df = generate_users()
    subscriptions_df = generate_subscriptions(users_df)
    
    # Save master data
    content_file = output_dir / 'content_catalog.parquet'
    users_file = output_dir / 'users.parquet'
    subscriptions_file = output_dir / 'subscriptions.parquet'
    
    save_to_parquet(content_df, content_file)
    save_to_parquet(users_df, users_file)
    save_to_parquet(subscriptions_df, subscriptions_file)
    
    # Upload master data to S3 if requested
    if args.upload_to_s3:
        print("\nUploading master data to S3...")
        upload_to_s3(str(content_file), 'raw/content/content_catalog.parquet', profile=args.profile)
        upload_to_s3(str(users_file), 'raw/users/users.parquet', profile=args.profile)
        upload_to_s3(str(subscriptions_file), 'raw/subscriptions/subscriptions.parquet', profile=args.profile)
    
    # Generate time-series data (viewership and revenue)
    print(f"\nGenerating time-series data for {NUM_DAYS} days...")
    for day_offset in range(NUM_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y%m%d")
        
        # Viewership events
        viewership_df = generate_viewership_events(
            current_date, users_df, content_df, NUM_VIEWERSHIP_EVENTS_PER_DAY
        )
        viewership_file = output_dir / f'viewership_{date_str}.parquet'
        save_to_parquet(viewership_df, viewership_file)
        
        # Revenue events
        revenue_df = generate_revenue_events(current_date, subscriptions_df, users_df)
        revenue_file = output_dir / f'revenue_{date_str}.parquet'
        save_to_parquet(revenue_df, revenue_file)
        
        # Upload to S3 with partitioning if requested
        if args.upload_to_s3:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")
            
            viewership_s3_key = f'raw/viewership/year={year}/month={month}/day={day}/viewership_{date_str}.parquet'
            revenue_s3_key = f'raw/revenue/year={year}/month={month}/day={day}/revenue_{date_str}.parquet'
            
            upload_to_s3(str(viewership_file), viewership_s3_key, profile=args.profile)
            upload_to_s3(str(revenue_file), revenue_s3_key, profile=args.profile)
    
    print()
    print("=" * 60)
    print("✅ Data Generation Complete!")
    print("=" * 60)
    print(f"Files saved to: {output_dir}")
    print(f"Total content items: {len(content_df)}")
    print(f"Total users: {len(users_df)}")
    print(f"Total subscriptions: {len(subscriptions_df)}")
    print(f"Total viewership events: {NUM_VIEWERSHIP_EVENTS_PER_DAY * NUM_DAYS:,}")
    print(f"Date range: {START_DATE.date()} to {(START_DATE + timedelta(days=NUM_DAYS-1)).date()}")
    
    if args.upload_to_s3:
        print(f"\n✅ Files uploaded to s3://{BUCKET_NAME}/raw/")
    print()


if __name__ == '__main__':
    main()

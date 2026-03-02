#!/usr/bin/env python3
"""
Convert generated data to CSV format for easy Snowflake upload
"""

import pandas as pd
import os

print("Converting sample data to CSV format for Snowflake upload...")

# Create output directory
os.makedirs('sample_data/snowflake_upload', exist_ok=True)

# Convert viewership from Parquet to CSV
print("  - Converting viewership.parquet to CSV...")
viewership_df = pd.read_parquet('sample_data/raw/viewership.parquet')
viewership_df.to_csv('sample_data/snowflake_upload/viewership.csv', index=False)
print(f"    ✓ Created viewership.csv ({len(viewership_df)} rows)")

# Copy revenue CSV
print("  - Copying revenue.csv...")
revenue_df = pd.read_csv('sample_data/raw/revenue.csv')
revenue_df.to_csv('sample_data/snowflake_upload/revenue.csv', index=False)
print(f"    ✓ Created revenue.csv ({len(revenue_df)} rows)")

# Convert content JSON to CSV
print("  - Converting content.json to CSV...")
content_df = pd.read_json('sample_data/raw/content.json', lines=True)
content_df.to_csv('sample_data/snowflake_upload/content.csv', index=False)
print(f"    ✓ Created content.csv ({len(content_df)} rows)")

# Copy users CSV
print("  - Copying users.csv...")
users_df = pd.read_csv('sample_data/raw/users.csv')
users_df.to_csv('sample_data/snowflake_upload/users.csv', index=False)
print(f"    ✓ Created users.csv ({len(users_df)} rows)")

print("\n✅ CSV files ready for Snowflake upload!")
print("\nFiles in ./sample_data/snowflake_upload/:")
print("  - viewership.csv")
print("  - revenue.csv")
print("  - content.csv")
print("  - users.csv")
print("\n📤 Upload these to Snowflake using the Web UI:")
print("   1. Go to Data → Databases → MEDIA_ANALYTICS → RAW")
print("   2. Click 'Create' → 'Table' → 'From File'")
print("   3. Upload each CSV file")
print("   4. Snowflake will create tables: VIEWERSHIP, REVENUE, CONTENT, USERS")

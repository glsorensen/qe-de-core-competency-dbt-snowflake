# 🚀 Quickstart Guide

Get up and running in 5 minutes!

## Prerequisites

- **Python 3.11** (dbt doesn't support 3.13+ yet)
- Slalom Snowflake access (SSO via your @slalom.com email)

## Steps

### 1. Clone & Setup

```bash
git clone https://github.com/glsorensen/qe-de-core-competency-dbt-snowflake.git
cd qe-de-core-competency-dbt-snowflake

# Create virtual environment with Python 3.11
python3.11 -m venv venv

# Activate it
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup Profile

```bash
cp profiles.yml.example profiles.yml
```

Edit `profiles.yml`:

1. Replace `YOUR_EMAIL@SLALOM.COM` with your actual Slalom email
2. Replace `SALES_DATABASE_XX` with your database (e.g., `SALES_DATABASE_GLS`)

**Note:** This project uses SSO authentication (`authenticator: externalbrowser`). When you run dbt commands, a browser window will open for you to log in.

### 3. Run!

```bash
dbt deps      # Install packages
dbt build     # Load data, run models, run tests
```

You should see: `Done. PASS=71 WARN=0 ERROR=0` ✅

### 4. Explore

```bash
dbt docs generate
dbt docs serve
```

Visit http://localhost:8080 to explore your data models!

---

📚 See `README.md` for detailed documentation.

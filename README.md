# Data Quality & Monitoring Dashboard

A comprehensive data quality monitoring system built with Python, PostgreSQL, and Grafana. This project demonstrates end-to-end data pipeline development with automated quality checks and real-time visualization.

## 🎯 Project Overview

This project implements a production-grade data quality monitoring solution that:
- **Generates** synthetic transaction data with realistic quality issues
- **Ingests** data into PostgreSQL
- **Validates** data against quality expectations
- **Visualizes** quality metrics in real-time using Grafana dashboards

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Generation** | Python (Pandas, NumPy) | Create synthetic transaction data |
| **Data Storage** | PostgreSQL | Store raw data and validation results |
| **Data Validation** | Python (Custom validators) | Run quality checks on data |
| **Visualization** | Grafana | Dashboard for monitoring data health |

## 📋 Prerequisites

- Python 3.10+
- PostgreSQL 12+
- Grafana (free version)
- Git

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/data-quality-dashboard.git
cd data-quality-dashboard
```

### 2. Set Up Python Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Database Credentials

Copy `.env.example` to `.env` and update with your PostgreSQL credentials:
```bash
cp .env.example .env
```

Edit `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_quality_demo
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### 5. Set Up PostgreSQL

```bash
# Start PostgreSQL service (macOS)
brew services start postgresql

# Create database and user
psql -U postgres
CREATE DATABASE data_quality_demo;
ALTER USER postgres WITH PASSWORD 'your_password';
\q
```

### 6. Run the Pipeline

Navigate to the `src/` directory:
```bash
cd src
```

**Generate sample data:**
```bash
python generate_data.py
```

**Ingest data into PostgreSQL:**
```bash
python ingest.py
```

**Run data quality validations:**
```bash
python validate.py
```

### 7. Set Up Grafana

```bash
# Install Grafana (macOS)
brew install grafana

# Start Grafana service
brew services start grafana
```

- Open http://localhost:3000
- Login with `admin / admin`
- Change your password
- Add PostgreSQL as a data source (Settings → Data Sources)
- Import the dashboard (see Dashboard Setup below)

## 📊 Dashboard Setup

### Add PostgreSQL Data Source
1. Go to **Settings** (⚙️) → **Data Sources**
2. Click **Add data source**
3. Select **PostgreSQL**
4. Configure:
   - Host: `localhost:5432`
   - Database: `data_quality_demo`
   - User: `postgres`
   - Password: `your_password`
   - SSL Mode: `disable`
5. Click **Save & test**

### Create Visualization Panels

The dashboard includes three main panels:

<img width="1418" height="751" alt="image" src="https://github.com/user-attachments/assets/05b6c625-0ec2-4006-a1c7-4776b94dded5" />


#### Panel 1: Current Data Quality Score
**Query:**
**Visualization:** Stat (large number display)

#### Panel 2: Quality Score Trend
**Query:**
**Visualization:** Time series (line chart)

#### Panel 3: Failed Validations
**Query:**
**Visualization:** Bar chart

## 📁 Project Structure

```
data-quality-dashboard/
├── data/
│   └── sample_transactions.csv      # Generated sample data
├── src/
│   ├── config.py                   # Configuration & database settings
│   ├── generate_data.py            # Generate synthetic transaction data
│   ├── ingest.py                   # Load data into PostgreSQL
│   ├── validate.py                 # Run data quality validations
├── .env                            # Database credentials (DO NOT COMMIT)
├── .gitignore                      # Git ignore rules
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## 🔍 Data Quality Validations

The `validate.py` script runs the following checks:

1. **Table Row Count** - Ensures data exists (1-10,000 rows)
2. **Required Columns** - Verifies all expected columns are present
3. **Transaction ID Uniqueness** - Checks for duplicate transaction IDs
4. **Valid Status Values** - Ensures status is one of: `completed`, `pending`, `failed`, `cancelled`
5. **Positive Amounts** - Validates that transaction amounts are positive or null
6. **Email Format** - Simple validation that emails contain `@` symbol
7. **Non-Null User IDs** - Ensures user_id is always populated

**Current Data Quality Score: ~71.4%** (intentional data issues for testing)

## 🔄 Running the Pipeline

For manual execution:
```bash
cd src
python generate_data.py  # Generate new data
python ingest.py         # Load into database
python validate.py       # Run validations
```

## 📚 Learning Outcomes

This project demonstrates:
- ✅ Data pipeline architecture (ETL/ELT)
- ✅ Python data processing with Pandas & NumPy
- ✅ SQL & relational database design
- ✅ Data quality frameworks & validation patterns
- ✅ Real-time monitoring dashboards
- ✅ Production best practices (error handling, logging)

## 💡 Real-World Applications

This project mirrors real production systems at companies where:
- Data quality monitoring is critical for business operations
- Automated validations catch issues before they impact downstream processes
- Dashboards enable data teams to track SLAs and metrics
- Historical trends help identify patterns and recurring issues

## 🤝 Contributing

Feel free to fork, improve, and submit pull requests!

## 📝 License

This project is open source and available under the MIT License.

## 👨‍💻 About

Built as a portfolio project to demonstrate data engineering fundamentals. Perfect for learning ETL/ELT patterns, data validation, and monitoring practices.

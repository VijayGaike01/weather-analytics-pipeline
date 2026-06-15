# 🌦️ Weather Analytics Pipeline & Dashboard

An end-to-end Data Engineering project that automatically collects weather data from the OpenWeatherMap API, processes and enriches it through a multi-stage ETL pipeline, stores historical records in SQLite, validates data quality using automated tests, and powers an interactive Power BI dashboard.

---

## 🚀 Project Overview

This project demonstrates a complete modern data pipeline including:

* REST API Data Extraction
* Data Transformation & Enrichment
* Incremental Data Loading
* Historical Data Storage
* Automated Data Quality Validation
* Workflow Orchestration with GitHub Actions
* Business Intelligence Reporting with Power BI

The pipeline executes automatically every **2 hours**, continuously building a historical weather dataset.

---

## 🏗️ Architecture

```text
OpenWeatherMap API
        │
        ▼
Extract Weather Data
        │
        ▼
Transform & Enrich Data
        │
        ▼
SQLite Database
(weather.db)
        │
        ├── Data Quality Validation (Pytest)
        │
        └── Power BI Dataset Export
                    │
                    ▼
            weather_dataset.csv
                    │
                    ▼
            Power BI Dashboard
```

---

## 🛠️ Technology Stack

### Data Engineering

* Python 3.12
* Pandas
* SQLite
* Requests

### Automation

* GitHub Actions

### Testing

* Pytest

### Business Intelligence

* Power BI

### Data Source

* OpenWeatherMap API

---

## ⚙️ ETL Pipeline

### 1. Extract

Fetches current weather information for configured cities using the OpenWeatherMap API.

Collected data includes:

* Temperature
* Humidity
* Pressure
* Wind Speed
* Visibility
* Cloud Coverage
* Sunrise & Sunset Times
* Weather Conditions

---

### 2. Transform

Transforms and enriches raw weather data by creating:

* Temperature (°F)
* Wind Speed (km/h)
* Heat Index
* Daylight Hours
* Temperature Categories
* Humidity Categories
* Weather Severity Indicators
* Date & Time Dimensions

---

### 3. Load

Loads transformed records into SQLite.

Database:

```text
database/weather.db
```

Tables:

```text
weather_observations
etl_load_audit
```

Historical records are continuously appended, enabling long-term trend analysis.

---

## 📊 Data Quality Validation

Automated validation tests execute after each pipeline run.

Current checks include:

* Database Connectivity
* Table Existence Validation
* Row Count Verification
* Null City Detection
* Duplicate Record Detection
* Future Timestamp Validation

Validation is performed using Pytest and integrated into GitHub Actions.

---

## 🔄 Automation

The entire pipeline is orchestrated using GitHub Actions.

### Schedule

```text
Every 2 Hours
```

### Workflow Steps

1. Checkout Repository
2. Install Dependencies
3. Create Runtime Configuration
4. Inject API Key from GitHub Secrets
5. Execute ETL Pipeline
6. Run Data Validation Tests
7. Export Power BI Dataset
8. Commit Updated Database
9. Push Changes to Repository

---

## 📁 Project Structure

```text
weather-analytics-pipeline/

├── .github/
│   └── workflows/
│       └── weather_pipeline.yml
│
├── config/
│
├── database/
│   └── weather.db
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── processed_metadata/
│   └── load_metadata/
│
├── logs/
│
├── powerbi/
│   ├── weather_dataset.csv
│   └── Weather_Analytics_Platform.pbix
│
├── scripts/
│   ├── extract_weather.py
│   ├── transform.py
│   ├── load_weather.py
│   ├── export_powerbi_dataset.py
│   ├── master_pipeline.py
│   └── pipeline_config.py
│
├── sql/
│
├── tests/
│
├── requirements.txt
└── README.md
```

---

## 📈 Power BI Dashboard

The dashboard provides:

### Executive KPIs

* Total Records
* Cities Tracked
* Average Temperature
* Average Humidity
* Latest Observation

### Analytics

* Temperature Trends
* Humidity Trends
* Weather Condition Distribution
* City Comparisons
* Geographic Mapping

The dashboard consumes:

```text
powerbi/weather_dataset.csv
```

which is automatically generated from the SQLite database after every pipeline execution.

---

## 🔐 Configuration

A template configuration file is provided:

```text
config/config_template.json
```

API keys are securely managed through GitHub Secrets and are never stored in the repository.

---

## 🎯 Future Enhancements

Planned improvements include:

* Dynamic City Management
* PostgreSQL Migration
* Advanced Power BI Dashboards
* Star Schema Data Warehouse Design
* Real-Time Weather Monitoring
* Forecast Analytics
* Cloud Deployment

---

## 👨‍💻 Author

**Vijay Gaike**

Data Engineering Portfolio Project

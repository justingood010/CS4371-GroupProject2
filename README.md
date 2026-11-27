# CS4372 Group Project 2
SafeStay: An LLM-Assisted Trip Planner Built on Open City Data

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Task 1](#task-1-incident-data-pipeline-with-spark)
- [Task 2](#task-2-monthly-neighbourhood-outlook)

## Features
* **Police Incident Reports:** Compares crime levels in specific neighborhoods.
* **Hotel Search:** Utilizes live booking prices to find the cheapest options.
* **Weather:** Compares historical weather data for different times of the year.
* **Nearby Attractions:** Provides recommendations for attractions near your hotel.

## Prerequisites
Python 3.11+

## Task 1: Incident Data Pipeline with Spark

### Obtain a City of Chicago API key
[Make a City of Chicago Data account](data.cityofchicago.org) and request a free App Token in the developer settings.

### Fill in Your App Token
Paste your App Token into `.env.example` and rename to `.env`

### Create and Activate Virtual Environment
```powershell
python -m venv .venv
.\venv\Scripts\Activate.ps1
```

### Install Packages
```powershell
pip install -r requirements.txt
```

### Run Scripts
```powershell
python ./src/IncidentCleaning.py
python ./IncidentExtract.py
```

## Task 2: Monthly Neighbourhood Outlook

### Obtain an OpenAI API key
[Create a Groq account](https://console.groq.com) and request a free API key, then paste it into your `.env` file.

### Run Monthly Outlook Script
```powershell
python src/monthlyOutlook.py
```

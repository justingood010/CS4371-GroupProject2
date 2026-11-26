# CS4372 Group Project 2
SafeStay: An LLM-Assisted Trip Planner Built on Open City Data

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Task 1](#task-1-incident-data-pipeline-with-spark)

## Features
* **Police Incident Reports:** Compares crime levels in specific neighborhoods.
* **Hotel Search:** Utilizes live booking prices to find the cheapest options.
* **Weather:** Compares istorical weather data for different times of the year.
* **Nearby Attractions:** Provides recommendations for attractions near your hotel.

## Prerequisites
Python 3.11+

## Task 1: Incident Data Pipeline with Spark

### Obtain a City of Chicago API key
Make an account at data.cityofchicago.org and request a free App Token in the developer settings.

### Create Virtual Environment
```powershell
python 3 -m venv .venv
source .venv/bin/activate
```

### Install packages
```powershell
pip install -r requirements.txt
```

### Fill in environment variable
Paste your App Token into .env.example and rename to .env

### Run scripts
```powershell
python ./src/IncidentCleaning.py
python ./IncidentExtract.py
```
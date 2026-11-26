# CS4372 Group Project 2
# SafeStay: An LLM-Assisted Trip Planner Built on Open City Data

## Task 1: Incident Extraction and Cleaning

### Obtain a City of Chicago API key
Make an account at data.cityofchicago.org and request a free App Token in the developer settings.

### Create Virtual Environment
`python 3 -m venv .venv`
`source .venv/bin/activate`

### Install packages
`pip install -r requirements.txt`

### Fill in environment variable
Paste your App Token into .env.example and rename to .env

### Run scripts
`python ./src/IncidentCleaning.py`
`python ./IncidentExtract.py`
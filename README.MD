# Spotify ETL Pipeline: Extract, Transform, and Load Data into PostgreSQL

## Overview

This repository implements an ETL (Extract, Transform, Load) pipeline that retrieves song data from Spotify's API based on the user's last 24 hours of activity. The data is transformed and loaded into a PostgreSQL database for storage and further analysis. The process is automated to run daily using CRON.

### Technologies Used:

- **Spotify API** (OAuth 2.0 Authentication)
- **Python** (for data extraction, transformation, and loading)
- **PostgreSQL** (for database storage)
- **CRON** (for scheduling automation)

## Requirements

### 1. Developer Setup on Spotify API

To use the Spotify API, you must first set up a Spotify Developer account and create an application to get your `client_id` and `client_secret`.

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/applications).
2. Log in with your Spotify account and click on "Create an App".
3. Provide necessary information (App Name, Description, etc.).
4. Once the app is created, you’ll have access to:
   - **Client ID**
   - **Client Secret**
   - **Redirect URI** (You’ll need to set up one if it’s not automatically provided)
5. Save these credentials for later use in the `.env` file.

### 2. Installing Requirements

Before running the ETL pipeline, you need to install the necessary dependencies. Follow these steps:

#### Clone the repository:

#### Create a virtual environment:

```bash
python3 -m venv env

source env/bin/activate
```

#### Install the dependencies:

```bash
pip install -r requirements.txt
```

### 3. Setup Environment Variables

To securely store your credentials and other configuration variables, create a .env file based on the .env.example template.

1. Copy the example .env file:

```bash
cp .env.example .env
```

2. Update the .env file with your Spotify credentials and PostgreSQL connection details:

### 4. Running the ETL Pipeline

To run the ETL pipeline, execute the following command:

```bash
python main.py
```

### 5. Automating the ETL Pipeline

To automate the ETL pipeline to run daily, you can use CRON jobs. To set up a CRON job, follow these steps:

1. Open the CRON tab for editing:

```bash
crontab -e
```

2. Add the following line to the CRON tab to run the ETL pipeline every day at 12:00 AM:

```bash
0 0 * * * /bin/bash/ /path/to/your/virtualenv/bin/python /path/to/your/repository/main.py
```

3. Save and exit the CRON tab.

### 6. Viewing the Data in UI using Streamlit

To view the data in a user-friendly interface, you can use Streamlit. To run the Streamlit app, execute the following command:

```bash
streamlit run frontend.py
```

for fetch data from database use fastapi running using below command

```bash
uvicorn server:app --reload
```

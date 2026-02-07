#!/bin/bash

# 1. Set environment variables
export AIRFLOW_HOME=$(pwd)
export PYTHONPATH=$PYTHONPATH:$(pwd)

# FIX: Tell Airflow exactly where your DAGs are (the /dags folder)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# FIX: Load your .env variables into the shell so Airflow can see them
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
  echo "✅ .env variables loaded"
fi

# 2. Initialize Database (only if it doesn't exist)
if [ ! -f "airflow.db" ]; then
    echo "🚀 Initializing Airflow database..."
    airflow db init
    
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Aditya \
        --lastname Sahu \
        --role Admin \
        --email aditya@example.com \
        --password admin
fi

# 3. Clean up old PID files (prevents "Airflow is already running" errors)
find . -name "*.pid" -delete

# 4. Start the Scheduler in the background
echo "🧠 Starting Airflow Scheduler..."
airflow scheduler > scheduler.log 2>&1 &

# 5. Start the Webserver
echo "🌐 Starting Airflow Webserver on http://localhost:8080..."
airflow webserver --port 8080
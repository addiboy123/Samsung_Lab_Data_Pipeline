#!/bin/bash

# 0. Activate venv so 'airflow' is on PATH (use project venv if present)
if [ -f "sam/bin/activate" ]; then
  source sam/bin/activate
fi

# 1. Set environment variables
export AIRFLOW_HOME=$(pwd)
export PYTHONPATH=$PYTHONPATH:$(pwd)

# FIX: Tell Airflow exactly where your DAGs are (the /dags folder)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# FIX: Load .env without breaking on spaces or special chars in values
if [ -f .env ]; then
  while IFS= read -r line; do
    line="${line%%#*}"   # strip trailing # comment
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.*)$ ]]; then
      export "${BASH_REMATCH[1]}=${BASH_REMATCH[2]}"
    fi
  done < .env
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

# 4. If port 8080 is in use, warn and offer to free it (optional: uncomment next 2 lines to auto-kill)
# if command -v lsof >/dev/null 2>&1 && lsof -ti:8080 >/dev/null 2>&1; then
#   echo "⚠️  Port 8080 in use. Freeing it..." && lsof -ti:8080 | xargs kill -9 2>/dev/null; sleep 1
# fi

if command -v lsof >/dev/null 2>&1 && lsof -ti:8080 >/dev/null 2>&1; then
  echo "⚠️  Port 8080 is already in use. Stop the other process first, e.g.:"
  echo "   lsof -ti:8080 | xargs kill -9"
  echo ""
fi

# 5. Start the Scheduler in the background
echo "🧠 Starting Airflow Scheduler..."
airflow scheduler > scheduler.log 2>&1 &

# 6. Start the Webserver
echo "🌐 Starting Airflow Webserver on http://localhost:8080..."
airflow webserver --port 8080
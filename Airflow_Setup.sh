#!/bin/bash

set -e  # Exit script on any command failure
set -o pipefail  # Exit if any command in a pipeline fails
set -u  # Treat unset variables as an error

LOG_FILE="airflow_setup.log"
exec > "$LOG_FILE" 2>&1  # Redirect all output to log file

echo "🚀 Starting Airflow setup..."

# Step 1: Setup Python Virtual Environment
echo "🔹 Setting up Python virtual environment..."
if [ ! -d "airflow_venv" ]; then
    python3 -m venv airflow_venv
    echo "✅ Virtual environment created."
else
    echo "⚠ Virtual environment already exists. Skipping creation."
fi

source airflow_venv/bin/activate

# Step 2: Upgrade pip and install dependencies
echo "🔹 Upgrading pip and installing dependencies..."
pip install --upgrade pip setuptools wheel

echo "🔹 Installing required Python packages..."
pip install --no-cache-dir --force-reinstall -r requirements.txt

echo "✅ Dependencies installed successfully."

# Step 3: Verify package installations
echo "🔹 Verifying package installations..."
python -c "import pyspark; print('✅ PySpark version:', pyspark.__version__)"
python -c "import pandas; print('✅ Pandas version:', pandas.__version__)"

# Step 4: Configure MySQL Connection in Airflow
echo "🔹 Configuring MySQL Connection..."
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="mysql+pymysql://airflow:airflow@localhost:3306/airflow"

# Step 5: Initialize Airflow Database
echo "🔹 Initializing Airflow database..."
airflow db reset -y && airflow db init
echo "✅ Airflow database initialized."

# Step 6: Set DAGs Directory
echo "🔹 Setting DAGs directory..."
export AIRFLOW__CORE__DAGS_FOLDER="/Users/siddharthamahakur/PycharmProjects/SparkETLMaster/airflow/dags"
echo "✅ DAGs directory set to $AIRFLOW__CORE__DAGS_FOLDER"

# Step 7: Start Airflow services
echo "🔹 Starting Airflow services..."
airflow webserver --port 8080 &> airflow_webserver.log &
WEB_PID=$!
echo "✅ Airflow Webserver started (PID: $WEB_PID)"

airflow scheduler &> airflow_scheduler.log &
SCHEDULER_PID=$!
echo "✅ Airflow Scheduler started (PID: $SCHEDULER_PID)"

# Step 8: Create Airflow Admin User
echo "🔹 Creating Airflow admin user..."
airflow users delete --username admin || true  # Ignore errors if user doesn't exist
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
echo "✅ Airflow admin user created."

# Final Confirmation
echo "🎉 Airflow setup completed successfully!"
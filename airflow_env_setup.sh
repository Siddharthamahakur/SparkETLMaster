## 1. Setup Python Virtual Environment
# Create and activate a virtual environment for Airflow.
python3 -m venv venv
source venv/bin/activate

## 2. Upgrade pip and install required dependencies
# Ensures all required packages are installed.
pip install --upgrade pip setuptools wheel
pip install pyspark apache-airflow
pip install mysqlclient apache-airflow-providers-mysql pymysql pandas

## 3. Verify package installations
# Check installed versions to confirm successful installation.
python -c "import pyspark; print(pyspark.__version__)"
python -c "import pandas; print(pandas.__version__)"

## 4. Airflow Setup and Database Initialization
# Initialize and reset Airflow metadata database.
airflow version
airflow db reset
airflow db init

## 5. Start Airflow Services
# Starts essential Airflow services.
airflow webserver --port 8080 &
airflow scheduler

## 6. Manage Airflow Users
# Create and delete users.
airflow users delete --username admin
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

## 7. Airflow Configuration & Monitoring
# Lists configurations, users, connections, variables, plugins, and pools.
airflow config list
airflow users list
airflow info
airflow connections list
airflow connections add my_conn_id \
  --conn-type mysql \
  --conn-host localhost \
  --conn-login root \
  --conn-password mypassword \
  --conn-schema airflow

airflow variables list
airflow variables set my_var value123
airflow variables delete my_var
airflow plugins list
airflow pools list
airflow pools delete <name>
airflow pools set <name> <slots> <description>

airflow roles create <role_name>
airflow roles list
airflow providers list
airflow providers get <provider_name>

## 8. Managing DAGs
# Commands to trigger, pause, delete, and test DAGs.
airflow dags trigger my_dag
airflow dags pause my_dag
airflow dags unpause my_dag
airflow dags delete my_dag --yes
airflow dags test my_dag 2025-03-09

airflow dags backfill my_dag --start-date 2025-03-01 --end-date 2025-03-05 --parallelism 3
airflow dags backfill my_dag --start-date 2025-03-01 --end-date 2025-03-05 --ignore-dependencies

airflow dags list-runs --state queued
airflow dags state my_dag 2025-03-08 queued

## 9. Handling Failed Tasks & DAG Runs
# Retry failed tasks and rerun failed DAGs.
airflow tasks run my_dag my_task 2025-03-08
airflow dags backfill my_dag --start-date 2025-03-08 --end-date 2025-03-08
airflow dags state my_dag <execution_date> failed

## 10. Airflow DAG Optimization
# Optimize DAG execution settings.
parallelism = 32
dag_concurrency = 16

## 11. Triggering a DAG from Another DAG
# Use TriggerDagRunOperator to trigger a DAG from another DAG.
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_child_dag = TriggerDagRunOperator(
  task_id="trigger_child_dag",
  trigger_dag_id="child_dag",
  wait_for_completion=True,
  dag=dag
)

## 12. Stopping Airflow Services & Cleanup
# Stops Airflow services and cleans up processes.
pkill -9 -f airflow
airflow webserver --pid ~/airflow/airflow-webserver.pid --daemon stop
airflow scheduler --pid ~/airflow/airflow-scheduler.pid --daemon stop
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
ps aux | grep airflow

tail -f ~/airflow/logs/scheduler/latest/airflow-scheduler.log

airflow --help

## 13. MySQL Database Setup for Airflow
# Setup MySQL database for Airflow backend.
mysql -u root -p

CREATE DATABASE airflow_db;
CREATE USER 'root'@'localhost' IDENTIFIED BY 'root';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'root'@'localhost';
FLUSH PRIVILEGES;

ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
FLUSH PRIVILEGES;
EXIT;
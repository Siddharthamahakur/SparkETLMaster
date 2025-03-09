**`Run all tests using:`**

pytest tests/

`Configuring Airflow Connections:`

`1. Add Spark Connection:`

airflow connections add spark_default \
--conn-type spark \
--conn-host local \
--conn-port 7077

`2. Add MySQL Connection:`

airflow connections add mysql_default \
--conn-type mysql \
--conn-host localhost \
--conn-login root \
--conn-password root \
--conn-schema airflow



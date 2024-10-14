# Airflow-DBT Reverse ETL with Salesforce

This project provides a custom Airflow operator that integrates **DBT** (Data Build Tool) transformations and **Salesforce Bulk API** operations, enabling reverse ETL workflows. The solution allows data to be extracted from SQL databases, transformed with DBT, and loaded into Salesforce using a flexible bulk load process.

## Key Features
- **Custom Airflow Operator:** Extends the `SalesforceBulkOperator` to execute SQL queries using Airflow's `SqlHook`, allowing seamless integration of DBT-transformed data into Salesforce.
- **Flexible Data Sources:** Supports any SQL-compatible database, decoupling the solution from DBT while maintaining best practices for SQL transformations.
- **Bulk Operations Support:** Easily perform bulk inserts, updates, deletes, and upserts in Salesforce, with support for handling large datasets.
- **Scalable Pipelines:** Built to handle large data volumes efficiently using Airflow’s orchestration.

## Getting Started

### Prerequisites
- **Apache Airflow** installed and running. [Airflow Installation Guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- **DBT** for data transformation. [DBT Installation Guide](https://docs.getdbt.com/docs/introduction)
- **Salesforce** with Bulk API enabled. [Salesforce Bulk API Documentation](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/)
- **SQL-compatible Database** such as Postgres, MySQL, or any database Airflow can connect to.

### Installation

To use the custom operator in your Airflow environment, ensure that you have Apache Airflow installed and properly configured. Then, follow the steps below:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/reverse-etl-salesforce.git
    cd reverse-etl-salesforce
    ```

2. **Add the custom operator to your Airflow DAGs:**

    Copy the custom `SalesforceBulkSQLOperator` from this repository into your project’s `dags/lib` or `plugins` directory.

    ```bash
    cp custom_salesforce_operator.py $AIRFLOW_HOME/dags/lib
    ```

3. **Set up Airflow connections:**
   - Configure an Airflow connection for your database.
   - Set up your Salesforce credentials using Airflow's UI or environment variables.

### Example DAG

Here’s an example DAG that demonstrates the use of `SalesforceBulkSQLOperator` to perform bulk inserts into Salesforce after running SQL queries:

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from your_project.custom_salesforce_operator import SalesforceBulkSQLOperator

# Function to generate SQL queries based on your DBT model
def generate_sql_queries(start_date, end_date):
    # Generate SQL queries based on a date range
    queries = []
    for date in range(start_date, end_date + 1):
        queries.append(f"SELECT * FROM your_table WHERE date_column = '{date}'")
    return queries

# Define the Airflow DAG
with DAG(
    dag_id="dbt_model_salesforce_bulk_insert",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Run the DBT model job
    run_dbt_model = DbtCloudRunJobOperator(
        task_id="run_dbt_model",
        dbt_cloud_conn_id="dbt_cloud_conn",  # DBT Cloud connection ID in Airflow
        job_id=12345,  # Replace with your DBT Cloud job ID for SalesforceContactObject model
        wait_for_termination=True
    )

    # Generate SQL query from the DBT model output
    generate_queries = PythonOperator(
        task_id="generate_queries",
        python_callable=generate_sql_queries,
        op_kwargs={"start_date": "2024-01-01", "end_date": "2024-12-31"}
    )

    # Use SalesforceBulkSQLOperator to load DBT output into Salesforce
    for i, query in enumerate(generate_queries.output):
        bulk_insert_task = SalesforceBulkSQLOperator(
            task_id=f"bulk_insert_{i}",
            sql_conn_id="connid",
            sql=query,
            # ... other parameters
        )
    # Define the task dependencies
    run_dbt_model >> generate_queries >> bulk_insert_salesforce
```
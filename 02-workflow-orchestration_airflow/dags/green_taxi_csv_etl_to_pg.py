import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Import the core ETL logic from the includes directory
from includes.db_operations import create_initial_tables, merge_data
from includes.extract_transform import load_and_prep_staging, extract_and_transform


# --- DAG Configuration ---$
TAXI_TYPE = 'green'
POSTGRES_CONN_ID = 'postgres'

START_DATE = pendulum.datetime(2019, 1, 1, tz="UTC")  # Start historical load from Jan 1, 2019
END_DATE = pendulum.datetime(2021, 7, 31, tz="UTC")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
        dag_id=f'load_green_data_to_pg',
        default_args=default_args,
        description=f'Extract, transforms and load {TAXI_TYPE} taxi data into PostgreSQL.',
        schedule='@monthly',  # Runs once per month starting from START_DATE
        start_date=START_DATE,
        end_date=END_DATE,
        catchup=True,  # Run for all missed intervals (2019-01-01 up to today)
        max_active_runs=1,
        tags={'etl', 'postgres', 'dbt'},
) as dag:


    # Step 1: Ensure Staging and Final Tables Exist
    # This must run only once before any data loading occurs
    create_initial_tables_task = PythonOperator(
        task_id='create_initial_tables',
        python_callable=create_initial_tables,
        op_kwargs={
            'taxi_type': TAXI_TYPE,
            'conn_id': POSTGRES_CONN_ID,
        },
    )

    # Step 2: Extract & Transform (Download CSV into a Pandas DataFrame)
    # The execution_date is passed via context, which is essential for determining the month to download
    extract_task = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform,
        op_kwargs={
            'taxi_type': TAXI_TYPE,
            'execution_date_str': '{{ ds }}',  # Format: YYYY-MM-DD (e.g., 2019-01-01)
        },
    )

    # Step 3: Load Data into Staging Table and Prepare (Add unique_row_id)
    load_staging_task = PythonOperator(
        task_id='load_and_prep_staging',
        python_callable=load_and_prep_staging,
        op_kwargs={
            'df': extract_task.output,  # Pass the DataFrame output from the previous task
            'taxi_type': TAXI_TYPE,
            'execution_date_str': '{{ ds }}',
            'conn_id': POSTGRES_CONN_ID,
        },
    )

    # Step 4: Merge Staging Data into the Final Table (UPSERT)
    # This deduplicates the data using the unique_row_id and moves it to the final table
    merge_task = PythonOperator(
        task_id='merge_data_into_final_table',
        python_callable=merge_data,
        op_kwargs={
            'taxi_type': TAXI_TYPE,
            'conn_id': POSTGRES_CONN_ID,
        },
    )

    # Step 5: Run dbt models for further transformation
    # This requires dbt to be installed and configured in your Airflow environment
    # run_dbt_models = BashOperator(
    #     task_id="run_dbt_transformations",
    #     bash_command=f"dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt/profiles --vars 'taxi_type: {TAXI_TYPE}'",
    #     trigger_rule="all_success"
    # )

    # --- Define the Dependency Chain ---

    # 1. Table creation must finish first. Use a dummy task to ensure it runs only once.
    # We use .set_upstream(None) to detach it from the main flow's catchup behavior
    # This is a common pattern for setup tasks.

    # Flow for data loading (runs monthly)
    monthly_flow = extract_task >> load_staging_task >> merge_task

    create_initial_tables_task >> monthly_flow  # >> run_dbt_models

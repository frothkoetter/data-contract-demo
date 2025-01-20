from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from pendulum import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import subprocess
import time
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import ( SQLColumnCheckOperator, SQLTableCheckOperator, SQLCheckOperator,SQLExecuteQueryOperator,SQLValueCheckOperator)
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

_CONN_ID = "cdw-conn"
_TABLE_NAME = "dbt_airlinedata_demo_reports.airline_performance" 
# Define expected schema
EXPECTED_SCHEMA = {
    'airline_code': 'string',
    'airline_name': 'string',
    'total_flights': 'bigint',
    'avg_departure_delay': 'double',
    'avg_arrival_delay': 'double',
    'total_cancelled': 'bigint',
    'update_date': 'date'
}

SLA_RUNTIME_SECONDS = 23 

def compare_schema(**kwargs):
    expected_schema = kwargs['expected_schema']
    ti = kwargs['ti']
    current_schema = ti.xcom_pull(task_ids='describe_table')
    
    # Process SQL result
    current_schema_dict = {row[0]: row[1] for row in current_schema}
    
    # Compare schemas
    mismatched_columns = {
        col: (expected_schema[col], current_schema_dict.get(col))
        for col in expected_schema
        if expected_schema[col] != current_schema_dict.get(col)
    }
    
    if mismatched_columns:
        raise ValueError(f"Schema mismatch detected: {mismatched_columns}")
    else:
        print(f"Schema for table is as expected.")
        return "Schema match"

def call_data_contract_enforcing(table_name, success):
    """
    Function to call the external Python script with table_name and success parameter.
    Args:
        table_name (str): Name of the table to pass to the script
        success (bool): Success flag to pass to the script
    """
    # Prepare the command to call the external Python script
    command = [
        'python',  # Command to run Python
        '/app/mount/data-contract-airline-performance/data-contract-enforcing.py',  # Path to your Python script
        table_name,  # Table name to pass as an argument
        str(int(success))  # Success flag (convert to int for passing as argument)
    ]
    # Execute the script with the arguments
    result = subprocess.run(command, capture_output=True, text=True)
    # Capture and print the output of the Python script
    print("Script Output:", result.stdout)
    print("Script Error:", result.stderr)
    
    # Raise exception if there was an error
    if result.returncode != 0:
        raise Exception("Data contract enforcement failed.")

def sla_start_timer(**context):
    """Record the start time."""
    context['ti'].xcom_push(key='sla_start_time', value=time.time())

def sla_end_timer(**context):
    end_time = time.time()
    ti = context['ti']
    start_time = ti.xcom_pull(key='sla_start_time', task_ids='sla_start_timer')
    print(f"Start:{start_time}")
    print(f"End:{end_time}")
    # Ensure both times are not None
    if start_time is not None and end_time is not None:
        runtime = end_time - start_time
        if runtime > SLA_RUNTIME_SECONDS:
              raise ValueError(f"Query runtime {runtime:.2f} seconds exceeded SLA of {SLA_RUNTIME_SECONDS} seconds.")
        else:
              print(f"Query runtime {runtime:.2f} seconds is within SLA.")
    else:
        print("Error: start_time or end_time is None")


# Define default arguments for the DAG
default_args = {
    'owner': 'frothkoe',
    'start_date':datetime(2024, 4, 16),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email': 'abc@example.com'
}

# Instantiate the DAG
dag = DAG(
    'data-contract-airline-performance',
    default_args=default_args,
    description='A DAG to execute SQL query and capture results',
    schedule_interval='@daily',  # Run the DAG daily
    catchup=False,
    is_paused_upon_creation=False
)
# Checks 
describe_table = SQLExecuteQueryOperator(
        task_id='describe_table',
        dag = dag,
        sql=f"DESCRIBE {_TABLE_NAME}",
        conn_id=_CONN_ID
    )

validate_schema = PythonOperator(
        task_id='validate_schema',
	dag = dag,
        python_callable=compare_schema,
        op_kwargs={
            'expected_schema': EXPECTED_SCHEMA
        },
        provide_context=True,
    )
check_nulls_ranges = SQLColumnCheckOperator(
        task_id='check_nulls_ranges',
        dag = dag,
        conn_id=_CONN_ID,
        table=_TABLE_NAME,
        column_mapping={
             'airline_name': { "null_check": {"equal_to": 0}},
             'update_date': { "null_check": {"equal_to": 0}},
             'total_flights': { "null_check": {"equal_to": 0}, "min": {"geq_to":0} },
             'avg_departure_delay': { "null_check": {"equal_to": 0},"min": {"geq_to":-3}, "max": {"leq_to":30}},
             'avg_arrival_delay': { "null_check": {"equal_to": 0},"min": {"geq_to":-3},"max": {"leq_to":30} },
             'total_cancelled': { "null_check": {"equal_to": 0},"min": {"geq_to":0} },
		},
     )
check_consistency= SQLColumnCheckOperator(
        task_id="check_consistency",
        dag = dag,
        conn_id=_CONN_ID,
        table=_TABLE_NAME,
        column_mapping={
            "airline_code": {
            	 "distinct_check": {"geq_to": 2},
            },
        },
    )
check_volume = SQLTableCheckOperator(
        task_id="check_volume",
        dag = dag,
        conn_id=_CONN_ID,
        table=_TABLE_NAME,
        checks={
            "row_count_check": {"check_statement": "COUNT(*) >= 20"},
        },
    )

check_acceptable_range = SQLValueCheckOperator(
        task_id="check_acceptable_range",
        dag = dag,
        conn_id=_CONN_ID,
        sql=f"select avg(avg_departure_delay) FROM {_TABLE_NAME}", 
	pass_value=8,
	tolerance=0.15  #15% tollerance 
    )


# Define the freshness threshold (e.g., 24 hours ago from now)
freshness_threshold = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
# Freshness Check: Verify that recent data exists
check_freshness = SQLCheckOperator(
    task_id='check_freshness',
    dag = dag,
    conn_id=_CONN_ID,
    sql=f"""SELECT CASE 
                       WHEN MAX(update_date) >= '{freshness_threshold}' 
                       THEN 'True' 
                       ELSE 'False' 
                   END AS freshness_status
            FROM {_TABLE_NAME}
        """
)

# Add the task to send an email on failure
#send_notification = EmailOperator(
#        task_id='send_notification',
#    	dag = dag,
#        to=['your_email@example.com'],  # Change this to your desired email list
#        subject='Airflow Task Failure Notification',
#        html_content=f"Data Contract {_TABLE_NAME} validation has failed. Please check the logs for more details."
#    )

# Define task to call the Python program
#enforce_data_contract = PythonOperator(
#        task_id='enforce_data_contract',
#    	dag = dag,
#        python_callable=call_data_contract_enforcing,
#        op_args=[_TABLE_NAME, True],  # Example: table_name='airline_performance' and success=True
#    )

set_data_contract_status = CdeRunJobOperator(
    task_id='set_data_contract_status',
    job_name='data-contract-enforcing'
)

sla_start_timer_task = PythonOperator(
        task_id='sla_start_timer',
    	dag = dag,
        python_callable=sla_start_timer,
        provide_context=True
    )

sla_run_query_task = SQLExecuteQueryOperator(
        task_id='sla_run_query',
    	dag = dag,
        sql=f"SELECT *  FROM {_TABLE_NAME} ",  # Replace with your actual query
        show_return_value_in_logs=False,
        conn_id=_CONN_ID
    )

sla_end_timer_task = PythonOperator(
        task_id='sla_end_timer',
    	dag = dag,
        python_callable=sla_end_timer,
        provide_context=True
    )

# Define task dependencies
describe_table >> validate_schema >> [
	check_nulls_ranges, 
	check_volume, 
	check_consistency,
	check_acceptable_range,
	check_freshness] >> sla_start_timer_task >> sla_run_query_task >> sla_end_timer_task  >> set_data_contract_status

from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

# Import Variable to manage the latest version dynamically
from airflow.models import Variable

# Get the latest version from Airflow Variables
LATEST_VERSION = Variable.get("LATEST_DAG_VERSION", default_var="v2.0")

# Define version variables
VERSION_1 = 'v1.0'
VERSION_2 = 'v2.0'

# DAG Version 1
if LATEST_VERSION == VERSION_1:
    with DAG(
        f'tutorial_{VERSION_1}',  # Include version in the DAG name
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description=f'A simple tutorial DAG (version {VERSION_1})',
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example', VERSION_1],  # Add version as a tag
    ) as dag_v1:

        t1_v1 = BashOperator(
            task_id='print_date',
            bash_command='date',
        )

        t2_v1 = BashOperator(
            task_id='sleep',
            depends_on_past=False,
            bash_command='sleep 5',
            retries=3,
        )

        templated_command_v1 = dedent(
            """
            {% for i in range(5) %}
                echo "{{ ds }}"
                echo "{{ macros.ds_add(ds, 7)}}"
            {% endfor %}
            """
        )

        t3_v1 = BashOperator(
            task_id='templated',
            depends_on_past=False,
            bash_command=templated_command_v1,
        )

        t1_v1 >> [t2_v1, t3_v1]

# DAG Version 2 (Latest Version)
if LATEST_VERSION == VERSION_2:
    with DAG(
        f'tutorial_{VERSION_2}',  # Include version in the DAG name
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 2,  # Different retry count for version 2
            'retry_delay': timedelta(minutes=10),  # Different retry delay for version 2
        },
        description=f'A simple tutorial DAG (version {VERSION_2})',
        schedule=timedelta(days=2),  # Different schedule for version 2
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example', VERSION_2, 'latest'],  # Add "latest" tag for the latest version
    ) as dag_v2:

        t1_v2 = BashOperator(
            task_id='print_date',
            bash_command='date',
        )

        t2_v2 = BashOperator(
            task_id='sleep',
            depends_on_past=False,
            bash_command='sleep 10',  # Different sleep duration for version 2
            retries=2,
        )

        templated_command_v2 = dedent(
            """
            {% for i in range(3) %}
                echo "{{ ds }}"
                echo "{{ macros.ds_add(ds, 3)}}"
            {% endfor %}
            """
        )

        t3_v2 = BashOperator(
            task_id='templated',
            depends_on_past=False,
            bash_command=templated_command_v2,
        )

        t1_v2 >> [t2_v2, t3_v2]
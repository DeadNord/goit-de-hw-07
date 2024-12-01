from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.dates import days_ago
import random
import time

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    "medal_count_dag",
    default_args=default_args,
    description="DAG to count medals and insert into MySQL table",
    schedule_interval=None,
) as dag:

    # Task 1: Create the table if it doesn't exist
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="goit_mysql_db",
        sql="""
        CREATE TABLE IF NOT EXISTS medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    # Task 2: Randomly select a medal type
    def pick_medal():
        medal = random.choice(["Bronze", "Silver", "Gold"])
        return medal

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # Task 3: Branching based on the selected medal
    def branch_func(**kwargs):
        ti = kwargs["ti"]
        medal = ti.xcom_pull(task_ids="pick_medal")
        return f"calc_{medal}"

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_func,
        provide_context=True,
    )

    # Task 4: Count medals and insert into the table
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id="goit_mysql_db",
        sql="""
        INSERT INTO medal_counts (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id="goit_mysql_db",
        sql="""
        INSERT INTO medal_counts (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id="goit_mysql_db",
        sql="""
        INSERT INTO medal_counts (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # Task 5: Introduce a delay
    def generate_delay():
        # time.sleep(5)
        time.sleep(35)  # Delay of 35 seconds to test the sensor failure

    generate_delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule="none_failed_min_one_success",
    )

    # Task 6: Check if the newest record is not older than 30 seconds
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id="goit_mysql_db",
        sql="""
        SELECT 1 FROM medal_counts
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        poke_interval=5,
        timeout=60,
    )

    # Setting up task dependencies
    create_table >> pick_medal_task >> branching
    branching >> [calc_Bronze, calc_Silver, calc_Gold]
    (
        [calc_Bronze, calc_Silver, calc_Gold]
        >> generate_delay_task
        >> check_for_correctness
    )

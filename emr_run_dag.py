from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'emr_pyspark_job_existing_cluster',
    default_args=default_args,
    description='Run PySpark job on existing EMR cluster and terminate cluster',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Define the EMR cluster ID
EMR_CLUSTER_ID = 'j-FFY6HMQ03GBP'  # Replace with your EMR cluster ID

# PySpark step configuration
SPARK_STEP = [
    {
        'Name': 'pyspark_job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://my-airflow-sample-bucke/dags/s3-to-s3-process.py'  # Replace with your PySpark script path
            ],
        },
    }
]

add_pyspark_step = EmrAddStepsOperator(
    task_id='add_pyspark_step',
    job_flow_id=EMR_CLUSTER_ID,
    steps=SPARK_STEP,
    aws_conn_id='aws_default',
    dag=dag,
)

step_sensor = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_pyspark_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)


# Set task dependencies
add_pyspark_step >> step_sensor

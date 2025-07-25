import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {"owner": "na-tarasova"}

dag = DAG(
     dag_id="startde-project-na-tarasova-dag",
     start_date=datetime.datetime(2025, 5, 24),
     schedule=None,
     default_args = default_args
 )

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

submit = SparkKubernetesOperator(
        task_id='job_submit',
        namespace=K8S_SPARK_NAMESPACE,
        application_file='spark_submit.yaml',
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=dag
    )


sensor = SparkKubernetesSensor(
        task_id='job_sensor',
        namespace=K8S_SPARK_NAMESPACE,
        application_name='spark-job-na-tarasova-items',
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=dag
    )

items_datamart = SQLExecuteQueryOperator(
    task_id="items_datamart", conn_id=GREENPLUM_ID, sql="items_datamart.sql"
)

create_unreliable_sellers_view = SQLExecuteQueryOperator(
    task_id="create_unreliable_sellers_report_view", conn_id=GREENPLUM_ID, sql="unreliable_sellers_view.sql"
)

create_brands_report_view = SQLExecuteQueryOperator(
    task_id="create_brands_report_view", conn_id=GREENPLUM_ID, sql="item_brands_view.sql"
)

submit >> sensor >> items_datamart >> [create_unreliable_sellers_view, create_brands_report_view]
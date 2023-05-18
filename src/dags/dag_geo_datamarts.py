import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1)
}

dag_spark = DAG(
    dag_id = "create_datamarts_spark",
    default_args=default_args,
    schedule_interval=None
)

dm_users = SparkSubmitOperator(
    task_id = 'dm_users',
    dag = dag_spark,
    application = '/lessons/datamart_users.py',
    conn_id= 'yarn_spark',
    application_args = [ 
        "/user/johnafv/project_7/events_sample", 
        "/user/johnafv/project_7/geo", 
        "/user/johnafv/project_7/marts/mart_users"
        ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '1g'
)

# dm_zones = SparkSubmitOperator(
#     task_id = 'dm_zones',
#     dag = dag_spark,
#     application = '/lessons/datamart_zones.py',
#     conn_id= 'yarn_spark',
#     application_args = [ 
#         "/user/johnafv/data/geo/events_light/", 
#         "/user/johnafv/data/geo/geo.csv", 
#         "/user/johnafv/prod/datamart_zones/"
#         ],
#     conf={
#         "spark.driver.maxResultSize": "20g"
#     },
#     executor_cores = 1,
#     executor_memory = '1g'
# )

# dm_friends_recomendation = SparkSubmitOperator(
#     task_id = 'dm_friends_recomendation',
#     dag = dag_spark,
#     application = '/lessons/datamart_friends_recomendation.py',
#     conn_id= 'yarn_spark',
#     application_args = [ 
#         "/user/johnafv/data/geo/events_light/", 
#         "/user/johnafv/data/geo/geo.csv", 
#         "/user/johnafv/prod/datamart_friends_recomendation/"
#         ],
#     conf={
#         "spark.driver.maxResultSize": "20g"
#     },
#     executor_cores = 1,
#     executor_memory = '1g'
# )



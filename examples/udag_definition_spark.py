from datetime import datetime, timedelta
from airflow import DAG
from dss_airflow_utils.operators.python_operator import PythonOperator
from dss_airflow_utils.spark import SparkOperator
from dss_airflow_utils.dag_factory import dag_factory, DagConfig, SparkConfig

SPARK_WORKER = "spark2.4.4-python3.7-worker"
SPARK_CONFIG = SparkConfig(
    num_nodes=1,
    request_cpu_per_node=4,
    request_memory_per_node=2,
    worker_type=SPARK_WORKER,
)
spark_queue_default = {
    "worker_type": SPARK_WORKER,
    "request_memory": "16G",
    "request_cpu": "4",
}
spark_confs_default = {"spark.executor.cores": 4, "spark.executor.memory": "2G"}
default_args = {
    "owner": "red",
    "start_date": datetime(2017, 3, 20),
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "queue": {
        "request_memory": "16G",
        "request_cpu": "4",
        "worker_type": SPARK_WORKER,
    },
}


@dag_factory(DagConfig(SPARK_CONFIG))
def create_dag():
    with DAG(
            dag_id="example_flow_spark", schedule_interval=None, default_args=default_args
    ) as dag:
        from .lib.uni.flow import init_step
        from .lib.uflow_definition_spark import (
            clean_data,
            export_model,
            generate_features,
            get_dataABC,
            get_dataXYZ,
            get_rand,
            train_model_RED,
            train_model_ROEY,
        )

        init = PythonOperator(
            task_id="init", python_callable=init_step, provide_context=True
        )
        get_rand = PythonOperator(
            task_id="get_rand",
            python_callable=get_rand,
            op_kwargs={"name": "get_rand", "func_param": {}, "const_params": {}},
            provide_context=True,
        )
        get_dataABC = PythonOperator(
            task_id="get_dataABC",
            python_callable=get_dataABC,
            op_kwargs={
                "name": "get_dataABC",
                "func_param": {"get_rand": "rand"},
                "const_params": {},
            },
            provide_context=True,
        )
        clean_data_2 = PythonOperator(
            task_id="clean_data_2",
            python_callable=clean_data,
            op_kwargs={
                "name": "clean_data_2",
                "func_param": {"get_dataABC": "table"},
                "const_params": {},
            },
            provide_context=True,
        )
        get_dataXYZ = PythonOperator(
            task_id="get_dataXYZ",
            python_callable=get_dataXYZ,
            op_kwargs={
                "name": "get_dataXYZ",
                "func_param": {"get_rand": "rand"},
                "const_params": {},
            },
            provide_context=True,
        )
        clean_data = PythonOperator(
            task_id="clean_data",
            python_callable=clean_data,
            op_kwargs={
                "name": "clean_data",
                "func_param": {"get_dataXYZ": "table"},
                "const_params": {},
            },
            provide_context=True,
        )
        generate_features = PythonOperator(
            task_id="generate_features",
            python_callable=generate_features,
            op_kwargs={
                "name": "generate_features",
                "func_param": {"clean_data": "table2", "clean_data_2": "table1"},
                "const_params": {},
            },
            provide_context=True,
        )
        train_model_RED = SparkOperator(
            task_id="train_model_RED",
            func=train_model_RED,
            params={
                "name": "train_model_RED",
                "func_param": {"generate_features": "features"},
                "const_params": {},
            },
            queue=spark_queue_default,
            spark_confs=spark_confs_default,
            provide_context=True,
        )
        train_model_ROEY = SparkOperator(
            task_id="train_model_ROEY",
            func=train_model_ROEY,
            params={
                "name": "train_model_ROEY",
                "func_param": {"generate_features": "features"},
                "const_params": {},
            },
            queue=spark_queue_default,
            spark_confs=spark_confs_default,
            provide_context=True,
        )
        export_model_2 = SparkOperator(
            task_id="export_model_2",
            func=export_model,
            params={
                "name": "export_model_2",
                "func_param": {"train_model_RED": "model"},
                "const_params": {"path": "model1.csv"},
            },
            queue=spark_queue_default,
            spark_confs=spark_confs_default,
            provide_context=True,
        )
        export_model = SparkOperator(
            task_id="export_model",
            func=export_model,
            params={
                "name": "export_model",
                "func_param": {"train_model_ROEY": "model"},
                "const_params": {"path": "model2.csv"},
            },
            queue=spark_queue_default,
            spark_confs=spark_confs_default,
            provide_context=True,
        )

        init >> get_rand
        get_dataABC >> clean_data_2
        clean_data >> generate_features
        get_rand >> get_dataABC
        get_rand >> get_dataXYZ
        train_model_RED >> export_model_2
        train_model_ROEY >> export_model
        generate_features >> train_model_RED
        generate_features >> train_model_ROEY
        clean_data_2 >> generate_features
        get_dataXYZ >> clean_data

        return dag

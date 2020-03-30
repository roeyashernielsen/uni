from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from flow_definition_2 import (
    clean_data,
    export_model,
    generate_features,
    get_dataABC,
    get_dataXYZ,
    get_weird_features,
    train_model_RED,
    train_model_ROEY,
)

default_args = {"owner": "red", "start_date": datetime(2017, 3, 20)}

with DAG(
    dag_id="sample_flow_2", schedule_interval=None, default_args=default_args
) as dag:
    export_model_3 = PythonOperator(
        task_id="export_model_3",
        python_callable=export_model,
        op_kwargs={
            "name": "export_model_3",
            "func_param": {"train_model_ROEY": "model"},
        },
        provide_context=True,
    )
    export_model_2 = PythonOperator(
        task_id="export_model_2",
        python_callable=export_model,
        op_kwargs={
            "name": "export_model_2",
            "func_param": {"train_model_RED": "model"},
        },
        provide_context=True,
    )
    get_dataABC = PythonOperator(
        task_id="get_dataABC",
        python_callable=get_dataABC,
        op_kwargs={"name": "get_dataABC", "func_param": {}},
        provide_context=True,
    )
    get_weird_features = PythonOperator(
        task_id="get_weird_features",
        python_callable=get_weird_features,
        op_kwargs={"name": "get_weird_features", "func_param": {}},
        provide_context=True,
    )
    clean_data_3 = PythonOperator(
        task_id="clean_data_3",
        python_callable=clean_data,
        op_kwargs={
            "name": "clean_data_3",
            "func_param": {"get_weird_features": "table"},
        },
        provide_context=True,
    )
    clean_data_2 = PythonOperator(
        task_id="clean_data_2",
        python_callable=clean_data,
        op_kwargs={"name": "clean_data_2", "func_param": {"get_dataABC": "table"}},
        provide_context=True,
    )
    get_dataXYZ = PythonOperator(
        task_id="get_dataXYZ",
        python_callable=get_dataXYZ,
        op_kwargs={"name": "get_dataXYZ", "func_param": {}},
        provide_context=True,
    )
    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"name": "clean_data", "func_param": {"get_dataXYZ": "table"}},
        provide_context=True,
    )
    generate_features = PythonOperator(
        task_id="generate_features",
        python_callable=generate_features,
        op_kwargs={
            "name": "generate_features",
            "func_param": {
                "clean_data": "table2",
                "clean_data_3": "table3",
                "clean_data_2": "table1",
            },
        },
        provide_context=True,
    )
    train_model_RED_2 = PythonOperator(
        task_id="train_model_RED_2",
        python_callable=train_model_RED,
        op_kwargs={
            "name": "train_model_RED_2",
            "func_param": {"generate_features": "features"},
        },
        provide_context=True,
    )
    train_model_ROEY = PythonOperator(
        task_id="train_model_ROEY",
        python_callable=train_model_ROEY,
        op_kwargs={
            "name": "train_model_ROEY",
            "func_param": {"generate_features": "features"},
        },
        provide_context=True,
    )
    train_model_RED = PythonOperator(
        task_id="train_model_RED",
        python_callable=train_model_RED,
        op_kwargs={
            "name": "train_model_RED",
            "func_param": {"clean_data_3": "features"},
        },
        provide_context=True,
    )
    export_model = PythonOperator(
        task_id="export_model",
        python_callable=export_model,
        op_kwargs={
            "name": "export_model",
            "func_param": {"train_model_RED_2": "model"},
        },
        provide_context=True,
    )

    clean_data >> generate_features
    clean_data_3 >> generate_features
    train_model_RED_2 >> export_model
    train_model_ROEY >> export_model_3
    train_model_RED >> export_model_2
    get_dataABC >> clean_data_2
    generate_features >> train_model_RED_2
    generate_features >> train_model_ROEY
    clean_data_2 >> generate_features
    clean_data_3 >> train_model_RED
    get_dataXYZ >> clean_data
    get_weird_features >> clean_data_3

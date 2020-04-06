"""Test."""
from src.uni import get_logger
from src.uni.flow.uflow import UFlow
from src.uni.flow.ustep import UStep


@UStep
def data_ingestion(a):
    logger = get_logger()
    raw_data = ([1, 2, 3, a], a)
    logger.critical(f"raw_data={raw_data}")
    # logger.info(prefect.context.runs_md)
    return raw_data


@UStep
def data_cleaning(raw_data=None):
    logger = get_logger()
    logger.critical(f"raw_data={raw_data}")
    clean_data = [10, 20, 30, 40]
    logger.critical(f"clean_data={clean_data}")
    # logger.info(prefect.context.runs_md)
    return clean_data


@UStep
def feature_engineering(clean_data=None):
    logger = get_logger()
    logger.critical(f"clean_data={clean_data}")
    # logger.info(prefect.context.runs_md)
    return True


# with UFlow("Project_X") as flow:
# #     d = data_ingestion.step(a=4)
# #     d2 = data_cleaning.step(raw_data=d)
# #     feature_engineering.step(clean_data=d2)


with UFlow("Project_X") as flow:
    d = data_ingestion.step(a=4)
    d2 = data_cleaning.step(raw_data=d)
    feature_engineering.step(clean_data=d2)

flow.run()

# data_ingestion.run()
# print(data_ingestion())

# mlflow.set_experiment("Project_X")
# run = mlflow.start_run()
# with Flow("Project_X") as flow:
#     d = data_ingestion.step(run_id=run.info.run_id, a=4)
#     d2 = data_cleaning.step(run_id=run.info.run_id, data=d)
#     feature_engineering.step(run_id=run.info.run_id, clean_data=d2)
# flow.run()
# mlflow.end_run()
# print(data_ingestion(a=4))

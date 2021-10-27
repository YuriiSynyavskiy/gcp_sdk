from airflow.exceptions import AirflowSkipException
from dotenv import dotenv_values
from sqlalchemy import MetaData, Table, create_engine, literal, select

config = dotenv_values('.env')

engine = create_engine(
    f'bigquery://{config.get("PROJECT_ID")}',
    credentials_path=config.get('GOOGLE_APPLICATION_CREDENTIALS'),
)


def log_new_files(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    files = ti.xcom_pull(task_ids='get_new_files_from_storage')

    print(f'New files: {files}')


def landing_success(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    processed_files = ti.xcom_pull(task_ids='get_new_files_from_storage')

    print(f'Processed files: {processed_files}')


def if_no_files(ti, **kwargs):
    if ti.xcom_pull(task_ids='get_new_files_from_storage'):
        raise AirflowSkipException('There is no new files')


def landing_to_staging(landing_table_name, ti, **kwargs):
    landing_table = Table(
        f'{config.get("DATASET_LANDING_ID")}.{landing_table_name}',
        MetaData(bind=engine),
        autoload=True,
    )
    staging_table = Table(
        f'{config.get("DATASET_STAGING_ID")}.'
        f'{config.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )

    _select = select([*landing_table.c, literal(ti.run_id)])
    _insert = staging_table.insert().from_select(staging_table.c, _select)

    query = _insert.compile()

    with engine.connect() as conn:
        conn.execute(query)

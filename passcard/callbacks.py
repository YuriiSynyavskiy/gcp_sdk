from datetime import datetime

from airflow.exceptions import AirflowSkipException
from dotenv import dotenv_values
from sqlalchemy import MetaData, Table, create_engine, literal, select, text

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


def create_tmp_table_name(ti, **kwargs):
    ts = str(datetime.today().replace(microsecond=0).timestamp())[0:-2]
    name = '_'.join(['tmp', config.get('PASSCARD_TABLE_NAME'), ts])
    ti.xcom_push('tmp_dm_passcard', name)


def if_new_files(ti, **kwargs):
    if ti.xcom_pull(task_ids='get_new_files_from_storage'):
        return True

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

    fields_for_hash = ','.join([
        landing_table.c.person_id.name,
        landing_table.c.security_id.name,
        landing_table.c.start_date.name,
        landing_table.c.expires_at.name,
    ])

    _select = select([
        *landing_table.c,
        text(f'to_hex(md5(concat({fields_for_hash})))'),
        literal(ti.run_id)])
    _insert = staging_table.insert().from_select(staging_table.c, _select)

    query = _insert.compile()

    with engine.connect() as conn:
        conn.execute(query)

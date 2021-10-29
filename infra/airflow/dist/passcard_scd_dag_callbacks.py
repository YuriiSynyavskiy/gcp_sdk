from datetime import datetime

from airflow.exceptions import AirflowSkipException
from dotenv import dotenv_values
from sqlalchemy import MetaData, Table, create_engine, func, literal, select, text, column
from sqlalchemy.sql.functions import concat

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
    target_table = Table(
        f'{config.get("DATASET_TARGET_ID")}.'
        f'{config.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )

    columns_for_hash = [
        landing_table.c.person_id,
        landing_table.c.security_id,
        landing_table.c.start_date,
        landing_table.c.expires_at,
    ]
    _hash = func.to_hex(func.md5(concat(*columns_for_hash))).label('hash')
    _select = (
        select([
            *landing_table.c,
            _hash,
        ]).where(
            (
                (target_table.c.passcard_key == landing_table.c.passcard_key)
                & (target_table.c.hash != column('hash'))
            )
        )
    )  # TODO: fix the query (the same data duplicates)

    with engine.connect() as conn:
        rows = conn.execute(_select)

    if not rows.fetchall():
        _select = select([
            *landing_table.c,
            _hash,
        ])

    _insert = staging_table.insert().from_select(staging_table.c, _select)

    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE '
                     f'{config.get("DATASET_STAGING_ID")}.'
                     f'{config.get("PASSCARD_TABLE_NAME")}')
        conn.execute(_insert)


def staging_to_target(ti, **kwargs):
    staging_table = Table(
        f'{config.get("DATASET_STAGING_ID")}.'
        f'{config.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )
    target_table = Table(
        f'{config.get("DATASET_TARGET_ID")}.'
        f'{config.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )

    values_for_update = {
        target_table.c.effective_end_date.name: func.current_datetime(),
        target_table.c.current_flag.name: 'N',
    }
    _update = (
        target_table
        .update()
        .values(values_for_update)
        .where(
            target_table.c.passcard_key.in_(
                select([staging_table.c.id])
            )
        )
    )

    columns_for_select = [
        staging_table.c.id,
        func.generate_uuid(),
        staging_table.c.person_id,
        staging_table.c.security_id,
        staging_table.c.start_date,
        staging_table.c.expires_at,
        func.datetime_sub(func.current_datetime(),
                          text('INTERVAL 100 MILLISECOND'),
                          ),
        func.datetime('9999-12-31T23:59:59.999999'),
        literal('Y'),
        staging_table.c.hash,
    ]
    _select = select(columns_for_select)

    columns_for_insert = [
        target_table.c.passcard_key,
        target_table.c.dm_passcard_id,
        target_table.c.dm_person_id,
        target_table.c.dm_security_id,
        target_table.c.start_date,
        target_table.c.expires_at,
        target_table.c.effective_start_date,
        target_table.c.effective_end_date,
        target_table.c.current_flag,
        target_table.c.hash,
    ]
    _insert = target_table.insert().from_select([*columns_for_insert], _select)

    with engine.connect() as conn:
        conn.execute(_update)
        conn.execute(_insert)

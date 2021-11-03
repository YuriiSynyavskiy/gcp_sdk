import os
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, column, create_engine, func, join, \
    literal, select, text
from sqlalchemy.sql.functions import concat

from logger import get_logger

load_dotenv()

logger = get_logger('passcard_scd_dag')

engine = create_engine(
    f'bigquery://{os.environ.get("PROJECT_ID")}',
)


def log_new_files(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    files = ti.xcom_pull(task_ids='get_new_files_from_storage')

    print(f'New files: {files}')


def landing_success(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    processed_files = ti.xcom_pull(task_ids='get_new_files_from_storage')

    print(f'Processed files: {processed_files}')


def generate_tmp_table_name(ti, **kwargs):
    ts = str(datetime.today().replace(microsecond=0).timestamp())[0:-2]
    name = '_'.join(['tmp', os.environ.get('PASSCARD_TABLE_NAME'), ts])
    ti.xcom_push('tmp_dm_passcard', name)


def if_new_files(ti, **kwargs):
    if ti.xcom_pull(task_ids='get_new_files_from_storage'):
        return True

    raise AirflowSkipException('There is no new files')


def landing_to_staging(landing_table_name, ti, **kwargs):
    landing_table = Table(
        f'{os.environ.get("DATASET_LANDING_ID")}.{landing_table_name}',
        MetaData(bind=engine),
        autoload=True,
    )
    staging_table = Table(
        f'{os.environ.get("DATASET_STAGING_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )
    target_table = Table(
        f'{os.environ.get("DATASET_TARGET_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")}',
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

    cte = select(
        [
            landing_table.c.id,
            landing_table.c.person_id,
            landing_table.c.security_id,
            landing_table.c.start_date,
            landing_table.c.expires_at,
            target_table.c.hash.label('target_hash'),
            _hash,
        ]
    ).select_from(
        join(
            landing_table,
            target_table,
            landing_table.c.id == target_table.c.passcard_key,
        ),
    ).where(target_table.c.current_flag == text('"Y"')).cte()

    select_updated = select([
        cte.c.id,
        cte.c.person_id,
        cte.c.security_id,
        cte.c.start_date,
        cte.c.expires_at,
        cte.c.hash,
    ]).select_from(cte).where(
        column('hash') != column('target_hash'),
    )

    # TODO: find a more elegant approach
    # BigQuery executes only INSERT INTO table WITH cte AS ...,
    # not WITH cte AS (...) INSERT ...
    insert_updated = text(
        f'INSERT INTO '
        f'{os.environ.get("DATASET_STAGING_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")} '
        f'{str(select_updated)}'
    )

    # TODO: try to use UNION
    select_new = select([
        *landing_table.c,
        _hash,
    ]).where(
        landing_table.c.id.notin_(
            select([target_table.c.passcard_key])
        )
    )

    insert_new = (
        staging_table
        .insert()
        .from_select(
            staging_table.c,
            select_new,
        )
    )

    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE '
                     f'{os.environ.get("DATASET_STAGING_ID")}.'
                     f'{os.environ.get("PASSCARD_TABLE_NAME")}')
        conn.execute(insert_updated)
        conn.execute(insert_new)


def staging_to_target(ti, **kwargs):
    staging_table = Table(
        f'{os.environ.get("DATASET_STAGING_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")}',
        MetaData(bind=engine),
        autoload=True,
    )
    target_table = Table(
        f'{os.environ.get("DATASET_TARGET_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")}',
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

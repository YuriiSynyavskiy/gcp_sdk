import os
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from google.cloud.bigquery import Client as BigQueryClient
from sqlalchemy import MetaData, Table, column, create_engine, func, join, \
    literal, select, text
from sqlalchemy.sql.functions import concat

from logger import get_logger
from utils import log_info

load_dotenv()

logger = get_logger('passcard_scd_dag')

engine = create_engine(
    f'bigquery://{os.environ.get("PROJECT_ID")}',
)


def log_new_files(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    files = ti.xcom_pull(task_ids='get_new_files_from_storage')

    log_info(logger, f'Files to process in the session: {files}', ti.run_id)


def log_landing_success(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    table_name = ti.xcom_pull(key='tmp_dm_passcard')

    log_info(
        logger,
        f'Data landed into table `{table_name}` at {datetime.now(tz=None)}',
        ti.run_id,
    )


def log_staging_success(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance
    n_updated = ti.xcom_pull(key='n_updated')
    n_new = ti.xcom_pull(key='n_new')

    log_info(
        logger,
        f'Staged {n_new} new records and {n_updated} records for updating',
        ti.run_id,
    )


def log_targeting_success(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance

    log_info(
        logger,
        f'Data transfered to DWH at {datetime.now(tz=None)}',
        ti.run_id,
    )


def log_landing_success_cleaning(context, **kwargs):
    ti = context.get('ti')  # type: airflow.models.TaskInstance

    n_deleted = ti.xcom_pull(key='n_deleted')

    log_info(
        logger,
        f'Deleted {n_deleted} temporary passcards tables',
        ti.run_id,
    )


def generate_tmp_table_name(ti, **kwargs):
    ts = str(datetime.today().replace(microsecond=0).timestamp())[0:-2]
    name = '_'.join(['tmp', os.environ.get('PASSCARD_TABLE_NAME'), ts])

    ti.xcom_push('tmp_dm_passcard', name)


def drop_tmp_passcards_tables(ti, **kwargs):
    client = BigQueryClient()

    tables = client.list_tables(os.environ.get('DATASET_LANDING_ID'))
    tables = [t for t in tables if 'tmp_dm_passcard' in t.table_id]

    for table in tables:
        client.delete_table(
            '.'.join([table.project, table.dataset_id, table.table_id]),
            not_found_ok=True,
        )

    ti.xcom_push('n_deleted', len(tables))


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

    # TODO: consider duplicates (i.e., from several files at time)
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

    select_new = select([
        *landing_table.c,
        _hash,
    ]).where(
        landing_table.c.id.notin_(
            select([target_table.c.passcard_key])
        )
    )

    united_selects = select_updated.union(select_new)

    insert_ = text(
        f'INSERT INTO '
        f'{os.environ.get("DATASET_STAGING_ID")}.'
        f'{os.environ.get("PASSCARD_TABLE_NAME")} '
        f'{str(united_selects)}'
    )
    # TODO: find a more elegant approach
    # BigQuery executes only INSERT INTO table WITH cte AS ...,
    # not WITH cte AS (...) INSERT ...

    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE '
                     f'{os.environ.get("DATASET_STAGING_ID")}.'
                     f'{os.environ.get("PASSCARD_TABLE_NAME")}')
        conn.execute(insert_)

        # TODO: maybe non-optimal
        n_updated = conn.execute(func.count(select_updated.c.id)).scalar()
        n_new = conn.execute(func.count(select_new.c.id)).scalar()

    ti.xcom_push('n_updated', n_updated)
    ti.xcom_push('n_new', n_new)


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
    update_ = (
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
    select_ = select(columns_for_select)

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
    insert_ = target_table.insert().from_select([*columns_for_insert], select_)

    with engine.connect() as conn:
        conn.execute(update_)
        conn.execute(insert_)

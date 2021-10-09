from person_dist.tables import landing_table, staging_table, target_table
from airflow.models import Variable


sql_landing_to_staging = f"""
    INSERT INTO {Variable.get('DATASET_ID')}.{staging_table} 
    SELECT b.hash_id, b.person_id, b.department_id, b.position_id, b.name, b.surname, 
    b.salary, b.phone, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.start_date) as start_date, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.end_date) as end_date
    FROM {Variable.get('DATASET_ID')}.{target_table} a right join ( 
    select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(end_date, '' )))) as hash_id, 
    person_id, department_id, position_id, name, surname, salary, phone, start_date, end_date 
    FROM {Variable.get('DATASET_ID')}.{landing_table} ) b on a.person_id = b.person_id 
    WHERE a.hash_id is null or a.current_flag='Y' and a.hash_id != b.hash_id; 
"""


sql_staging_to_target = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{target_table} t 
    set 
        t.current_dm_department_id = a.department_id 
    from {Variable.get('DATASET_ID')}.{staging_table} a 
    where a.person_id = t.person_id  and t.current_flag='N';

    update {Variable.get('DATASET_ID')}.{target_table} t 
    set 
        t.effective_end_date = current_datetime(),
        t.current_flag='N',
        t.prev_dm_department_id = t.current_dm_department_id,
        t.current_dm_department_id = a.department_id 
    from {Variable.get('DATASET_ID')}.{staging_table} a 
    where a.person_id = t.person_id and t.current_flag='Y';

    insert into {Variable.get('DATASET_ID')}.{target_table} 
    select hash_id, generate_uuid(), person_id, department_id, department_id, position_id, name, surname, salary, phone, start_date, end_date, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y' 
    from {Variable.get('DATASET_ID')}.{staging_table};

    delete from {Variable.get('DATASET_ID')}.{staging_table} where true; 
    delete from {Variable.get('DATASET_ID')}.{landing_table} where true; 
    COMMIT TRANSACTION;
    """


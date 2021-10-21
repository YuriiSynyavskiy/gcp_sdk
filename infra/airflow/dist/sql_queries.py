from dist.tables import (landing_location_table, staging_location_table, target_location_table, dept_table_name, 
                        tmp_dept_table_name, person_table_name, tmp_person_table_name)

from airflow.models import Variable

sql_temporary_to_landing_person = f"""
    INSERT INTO {Variable.get('LANDING_DATASET_ID')}.{person_table_name} 
    SELECT '{{{{run_id}}}}', person_key, department_id, position_id, name, surname, salary, phone, start_date, end_date 
    FROM {Variable.get('LANDING_DATASET_ID')}.{tmp_person_table_name};
"""


sql_landing_to_staging_person = f"""
    BEGIN TRANSACTION;

    delete from {Variable.get('STAGING_DATASET_ID')}.{person_table_name} where true;     
    delete from {Variable.get('LANDING_DATASET_ID')}.{tmp_person_table_name} where true;

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{person_table_name} 
    SELECT b.person_key, b.department_id, b.position_id, b.name, b.surname, 
    b.salary, b.phone, b.start_date as start_date, b.end_date as end_date,
    b.hash_key 
    FROM {Variable.get('DATASET_ID')}.{person_table_name} a right join ( 
    select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(format_date('%Y-%m-%d', end_date), '' )))) as hash_key, 
    person_key, department_id, position_id, name, surname, salary, phone, start_date, end_date, run_id 
    FROM {Variable.get('LANDING_DATASET_ID')}.{person_table_name} 
    WHERE run_id = '{{{{run_id}}}}') b on a.person_key = b.person_key 
    WHERE a.hash_key is null or a.current_flag='Y' and a.hash_key != b.hash_key;

    COMMIT TRANSACTION;
"""

sql_staging_to_target_person = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{person_table_name} t 
    set 
        t.current_dm_department_id = a.department_id 
    from {Variable.get('STAGING_DATASET_ID')}.{person_table_name} a 
    where a.person_key = t.person_key and t.current_flag='N';

    update {Variable.get('DATASET_ID')}.{person_table_name} t 
    set 
        t.effective_end_date = current_datetime(),
        t.current_flag='N',
        t.prev_dm_department_id = t.current_dm_department_id,
        t.current_dm_department_id = a.department_id 
    from {Variable.get('STAGING_DATASET_ID')}.{person_table_name} a 
    where a.person_key = t.person_key and t.current_flag='Y';

    insert into {Variable.get('DATASET_ID')}.{person_table_name} 
    select generate_uuid(), person_key, department_id, department_id, position_id, name, surname, salary, phone, start_date, CAST(FORMAT_DATE("%Y%m%d", start_date) as INT) as start_date_id, end_date, CAST(FORMAT_DATE("%Y%m%d", end_date) as INT) as end_date_id, current_datetime(), datetime('9999-12-31T23:59:59.000000'), 'Y', hash_key
    from {Variable.get('STAGING_DATASET_ID')}.{person_table_name};

    COMMIT TRANSACTION;
    """

sql_temporary_to_landing_dept = f"""
    INSERT INTO {Variable.get('LANDING_DATASET_ID')}.{dept_table_name} 
    SELECT '{{{{run_id}}}}', department_key, building_id, name, description, parent_id 
    FROM {Variable.get('LANDING_DATASET_ID')}.{tmp_dept_table_name};
"""

sql_landing_to_staging_dept = f"""
    BEGIN TRANSACTION;
    delete from {Variable.get('STAGING_DATASET_ID')}.{dept_table_name} where true; 
    delete from {Variable.get('LANDING_DATASET_ID')}.{tmp_dept_table_name} where true; 

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{dept_table_name} 
    SELECT b.department_key, b.building_id, b.name, b.description, b.parent_id, b.hash_key 
    FROM {Variable.get('DATASET_ID')}.{dept_table_name} a right join ( 
    select to_hex(md5(concat(building_id, name, description, ifnull(parent_id, '' )))) as hash_key, 
    department_key, building_id, name, description, parent_id, run_id 
    FROM {Variable.get('LANDING_DATASET_ID')}.{dept_table_name} 
    WHERE run_id = '{{{{run_id}}}}') b on a.department_key = b.department_key 
    WHERE a.hash_key is null or a.current_flag='Y' and a.hash_key != b.hash_key; 

    COMMIT TRANSACTION;
    """

sql_staging_to_target_dept = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{dept_table_name} t 
    set 
        t.effective_end_date = current_datetime(),
        t.current_flag='N'
    from {Variable.get('STAGING_DATASET_ID')}.{dept_table_name} a 
    where a.department_key = t.department_key and t.current_flag='Y';

    insert into {Variable.get('DATASET_ID')}.{dept_table_name} 
    select generate_uuid(), department_key, building_id, name, description, parent_id, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y', hash_key
    from {Variable.get('STAGING_DATASET_ID')}.{dept_table_name};

    COMMIT TRANSACTION;
    """

sql_landing_to_staging_location = f"""
    INSERT INTO {Variable.get('DATASET_ID')}.{staging_location_table} 
    SELECT b.hash_id, b.location_id, b.building_id, b.security_id, b.gate_id, b.room_number, b.floor, b.description 
    FROM {Variable.get('DATASET_ID')}.{target_location_table} a right join ( 
    select to_hex(md5(concat(building_id, security_id, gate_id, room_number, floor, description))) as hash_id, 
    location_id, building_id, security_id, gate_id, room_number, floor, description
    FROM {Variable.get('DATASET_ID')}.{landing_location_table} ) b on a.location_id = b.location_id 
    WHERE a.hash_id is null or a.flag='Y' and a.hash_id != b.hash_id;
    """

sql_staging_to_target_location = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{target_location_table} t 
    set 
        t.effective_end_date = current_datetime(),
        t.flag='N'
    from {Variable.get('DATASET_ID')}.{staging_location_table} a 
    where a.location_id = t.location_id and t.flag='Y';

    insert into {Variable.get('DATASET_ID')}.{target_location_table} 
    select hash_id, generate_uuid(), location_id, building_id, security_id, gate_id, room_number, floor, description, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y' 
    from {Variable.get('DATASET_ID')}.{staging_location_table};

    delete from {Variable.get('DATASET_ID')}.{staging_location_table} where true; 
    delete from {Variable.get('DATASET_ID')}.{landing_location_table} where true; 
    COMMIT TRANSACTION;
    """

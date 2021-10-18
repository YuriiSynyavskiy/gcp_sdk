from dist.tables import (landing_person_table, staging_person_table, target_person_table,
                        landing_dept_table, staging_dept_table, target_dept_table, landing_location_table,
                        staging_location_table, target_location_table)

from airflow.models import Variable


sql_landing_to_staging = f"""
    INSERT INTO {Variable.get('DATASET_ID')}.{staging_person_table} 
    SELECT b.hash_id, b.person_id, b.department_id, b.position_id, b.name, b.surname, 
    b.salary, b.phone, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.start_date) as start_date, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.end_date) as end_date
    FROM {Variable.get('DATASET_ID')}.{target_person_table} a right join ( 
    select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(end_date, '' )))) as hash_id, 
    person_id, department_id, position_id, name, surname, salary, phone, start_date, end_date 
    FROM {Variable.get('DATASET_ID')}.{landing_person_table} ) b on a.person_id = b.person_id 
    WHERE a.hash_id is null or a.current_flag='Y' and a.hash_id != b.hash_id; 
"""


sql_staging_to_target = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{target_person_table} t 
    set 
        t.current_dm_department_id = a.department_id 
    from {Variable.get('DATASET_ID')}.{staging_person_table} a 
    where a.person_id = t.person_id  and t.current_flag='N';

    update {Variable.get('DATASET_ID')}.{target_person_table} t 
    set 
        t.effective_end_date = current_datetime(),
        t.current_flag='N',
        t.prev_dm_department_id = t.current_dm_department_id,
        t.current_dm_department_id = a.department_id 
    from {Variable.get('DATASET_ID')}.{staging_person_table} a 
    where a.person_id = t.person_id and t.current_flag='Y';

    insert into {Variable.get('DATASET_ID')}.{target_person_table} 
    select hash_id, generate_uuid(), person_id, department_id, department_id, position_id, name, surname, salary, phone, start_date, end_date, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y' 
    from {Variable.get('DATASET_ID')}.{staging_person_table};

    delete from {Variable.get('DATASET_ID')}.{staging_person_table} where true; 
    delete from {Variable.get('DATASET_ID')}.{landing_person_table} where true; 
    COMMIT TRANSACTION;
    """

sql_landing_to_staging_dept = f"""
    INSERT INTO {Variable.get('DATASET_ID')}.{staging_dept_table} 
    SELECT b.hash_id, b.department_id, b.building_id, b.name, b.description, b.parent_id 
    FROM {Variable.get('DATASET_ID')}.{target_dept_table} a right join ( 
    select to_hex(md5(concat(building_id, name, description, ifnull(parent_id, '' )))) as hash_id, 
    department_id, building_id, name, description, parent_id 
    FROM {Variable.get('DATASET_ID')}.{landing_dept_table} ) b on a.department_id = b.department_id 
    WHERE a.hash_id is null or a.current_flag='Y' and a.hash_id != b.hash_id; 
    """

sql_staging_to_target_dept = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{target_dept_table} t 
    set 
        t.effective_end_date = current_datetime(),
        t.current_flag='N'
    from {Variable.get('DATASET_ID')}.{staging_dept_table} a 
    where a.department_id = t.department_id and t.current_flag='Y';

    insert into {Variable.get('DATASET_ID')}.{target_dept_table} 
    select hash_id, generate_uuid(), department_id, building_id, name, description, parent_id, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y' 
    from {Variable.get('DATASET_ID')}.{staging_dept_table};

    delete from {Variable.get('DATASET_ID')}.{staging_dept_table} where true; 
    delete from {Variable.get('DATASET_ID')}.{landing_dept_table} where true; 
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

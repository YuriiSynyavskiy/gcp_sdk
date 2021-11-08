from dist.tables import (location_table_name, dept_table_name, 
                        tmp_dept_table_name, person_table_name, 
                        tmp_person_table_name, gate_table_name, 
                        datamart_throughput_table_name, fk_passage_table_name,
                        error_fk_passage_table_name, passcard_table_name,
                        datamart_gate_analysis_table_name)

from dist.logger import get_logger

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
    SELECT b.person_key, c.dm_department_id, b.position_id, b.name, b.surname, 
    b.salary, b.phone, b.start_date as start_date, b.end_date as end_date,
    b.hash_key 
    FROM {Variable.get('DATASET_ID')}.{person_table_name} a right join ( 
    select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(format_date('%Y-%m-%d', end_date), '' )))) as hash_key, 
    person_key, department_id, position_id, name, surname, salary, phone, start_date, end_date, run_id 
    FROM {Variable.get('LANDING_DATASET_ID')}.{person_table_name} 
    WHERE run_id = '{{{{run_id}}}}') b on a.person_key = b.person_key left join {Variable.get('DATASET_ID')}.{dept_table_name} c on b.department_id  = c.department_key and c.current_flag='Y' 
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
    BEGIN TRANSACTION;
    delete from {Variable.get('STAGING_DATASET_ID')}.{location_table_name} where true; 

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{location_table_name} 
    SELECT b.location_key, b.building_id, b.security_id, c.dm_gate_id, b.room_number, b.floor, b.description, b.hash_id
    FROM {Variable.get('DATASET_ID')}.{location_table_name} a right join ( 
    select to_hex(md5(concat(run_id, building_id, security_id, gate_id, room_number, floor, description))) as hash_id, 
    location_key, building_id, security_id, gate_id, room_number, floor, description
    FROM {Variable.get('LANDING_DATASET_ID')}.{location_table_name} ) b on a.location_key = b.location_key left join {Variable.get('DATASET_ID')}.{gate_table_name} c on b.gate_id = c.gate_key and c.flag='Y'
    WHERE a.hash_id is null or a.flag='Y' and a.hash_id != b.hash_id;

    COMMIT TRANSACTION;
    """

sql_staging_to_target_location = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{location_table_name} t 
    set 
        t.effective_end_date = current_datetime(),
        t.flag='N'
    from {Variable.get('STAGING_DATASET_ID')}.{location_table_name} a 
    where a.location_key = t.location_key and t.flag='Y';

    insert into {Variable.get('DATASET_ID')}.{location_table_name} 
    select generate_uuid(), location_key, building_id, security_id, gate_id, room_number, floor, description, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y', hash_id
    from {Variable.get('STAGING_DATASET_ID')}.{location_table_name};

    COMMIT TRANSACTION;
    """

sql_landing_to_staging_gate = f"""
    BEGIN TRANSACTION;
    delete from {Variable.get('STAGING_DATASET_ID')}.{gate_table_name} where true; 

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{gate_table_name} 
    SELECT b.gate_key, b.contact_information, b.state, b.throughput, b.hash_id
    FROM {Variable.get('DATASET_ID')}.{gate_table_name} a right join ( 
    select to_hex(md5(concat(run_id, contact_information, state, throughput))) as hash_id, 
    gate_key, contact_information, state, throughput
    FROM {Variable.get('LANDING_DATASET_ID')}.{gate_table_name} ) b on a.gate_key = b.gate_key 
    WHERE a.hash_id is null or a.flag='Y' and a.hash_id != b.hash_id;

    COMMIT TRANSACTION;
    """

sql_staging_to_target_gate = f"""
    BEGIN TRANSACTION;
    update {Variable.get('DATASET_ID')}.{gate_table_name} t 
    set 
        t.effective_end_date = current_datetime(),
        t.state='broken',
        t.flag='N'
    from {Variable.get('STAGING_DATASET_ID')}.{gate_table_name} a 
    where a.gate_key = t.gate_key and t.state = 'working' and t.flag='Y';

    insert into {Variable.get('DATASET_ID')}.{gate_table_name} 
    select generate_uuid(), gate_key, contact_information, state, throughput, current_datetime(), datetime('9999-12-31T23:59:59'), 'Y', hash_id
    from {Variable.get('STAGING_DATASET_ID')}.{gate_table_name};

    COMMIT TRANSACTION;
    """

sql_update_throughput_datamart = f"""
    INSERT INTO {Variable.get("DATAMART_DATASET_ID")}.{datamart_throughput_table_name} 
    SELECT a.dm_gate_id, b.gate_key, timestamp, cast(throughput as int) 
    FROM {Variable.get('DATASET_ID')}.{fk_passage_table_name} a left join {Variable.get('DATASET_ID')}.{gate_table_name} b on a.dm_gate_id = b.dm_gate_id 
    WHERE timestamp > '{{{{task_instance.xcom_pull(task_ids="get_last_datamarts_updates", key="{datamart_throughput_table_name}")}}}}'
"""

sql_landing_to_staging_fk_passage = f"""
    BEGIN TRANSACTION;
    delete from {Variable.get('STAGING_DATASET_ID')}.{fk_passage_table_name} where true; 

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{fk_passage_table_name} 
    SELECT main.id, main.dm_gate_key, a.dm_gate_id, main.dm_passcard_key,
           b.dm_passcard_id, main.dm_status_id, main.dm_direction_id, main.timestamp,
           CAST(FORMAT_TIMESTAMP('%Y%m%d', main.timestamp, 'UTC') as int) as dm_date_id, FORMAT_TIMESTAMP('%H%M%S', main.timestamp, 'UTC') as dm_time_id 
    FROM {Variable.get('LANDING_DATASET_ID')}.{fk_passage_table_name} main 
        left join {Variable.get('DATASET_ID')}.{gate_table_name} a on cast(main.dm_gate_key as string) = a.gate_key and a.flag = 'Y' 
        left join {Variable.get('DATASET_ID')}.{passcard_table_name} b on main.dm_passcard_key = b.passcard_key and b.current_flag = 'Y' 
    WHERE main.timestamp > '{{{{task_instance.xcom_pull(task_ids="get_last_updated_record", key="last_updated_date")}}}}';

    INSERT INTO {Variable.get('STAGING_DATASET_ID')}.{fk_passage_table_name} 
    SELECT main.id, main.dm_gate_key, a.dm_gate_id, main.dm_passcard_key,
           b.dm_passcard_id, main.dm_status_id, main.dm_direction_id, main.timestamp,
           CAST(FORMAT_TIMESTAMP('%Y%m%d', main.timestamp, 'UTC') as int) as dm_date_id, FORMAT_TIMESTAMP('%H%M%S', main.timestamp, 'UTC') as dm_time_id 
    FROM {Variable.get('DATASET_ID')}.{error_fk_passage_table_name} main 
        left join {Variable.get('DATASET_ID')}.{gate_table_name} a on cast(main.dm_gate_key as string) = a.gate_key and a.flag = 'Y' 
        left join {Variable.get('DATASET_ID')}.{passcard_table_name} b on main.dm_passcard_key = b.passcard_key and b.current_flag = 'Y'; 
    
    DELETE FROM {Variable.get('DATASET_ID')}.{error_fk_passage_table_name} where true;

    COMMIT TRANSACTION;
"""

sql_staging_to_target_fk_passage = f"""
    BEGIN TRANSACTION;

    INSERT INTO {Variable.get('DATASET_ID')}.{fk_passage_table_name}  
    SELECT id, dm_gate_key, dm_gate_id, dm_passcard_key, 
           dm_passcard_id, dm_status_id, dm_direction_id, timestamp,
           dm_date_id, dm_time_id 
    FROM {Variable.get('STAGING_DATASET_ID')}.{fk_passage_table_name}
    WHERE dm_gate_id is not null and dm_gate_id != '' and dm_passcard_id is not null and dm_passcard_id != '';

    INSERT INTO {Variable.get('DATASET_ID')}.{error_fk_passage_table_name} 
    SELECT id, dm_gate_key, dm_passcard_key, dm_status_id, dm_direction_id, timestamp 
    FROM {Variable.get('STAGING_DATASET_ID')}.{fk_passage_table_name}
    WHERE dm_gate_id is null or dm_gate_id = '' or dm_passcard_id is null or dm_passcard_id = '';

    COMMIT TRANSACTION;
"""

sql_update_gate_analysis_datamart = f"""
    INSERT INTO {Variable.get("DATAMART_DATASET_ID")}.{datamart_gate_analysis_table_name}
    SELECT a.dm_gate_id, b.dm_location_id, b.dm_building_id, b.room_number, b.floor, a.state, a.throughput, CURRENT_TIMESTAMP() as timestamp
    FROM {Variable.get('DATASET_ID')}.{gate_table_name} a left join ( 
    select dm_location_id,dm_gate_id,dm_building_id, room_number, floor FROM {Variable.get('DATASET_ID')}.{location_table_name} ) b on a.dm_gate_id = b.dm_gate_id
    where dm_location_id is not null and timestamp > '{{{{task_instance.xcom_pull(task_ids="get_last_datamarts_updates", key="{datamart_throughput_table_name}")}}}}';
"""

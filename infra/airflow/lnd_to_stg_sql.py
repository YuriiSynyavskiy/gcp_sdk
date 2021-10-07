from tables import landing_table, staging_table, target_table
from airflow.models import Variable

sql_landing_to_staging = f"""
    INSERT INTO {Variable.get('DATASET_ID')}.{staging_table} 
    SELECT b.hash_id, b.person_id, b.department_id, b.position_id, b.name, b.surname, 
    b.salary, b.phone, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.start_date), PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.end_date) 
    FROM {Variable.get('DATASET_ID')}.{target_table} a right join ( 
    select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(end_date, '' )))) as hash_id, 
    person_id, department_id, position_id, name, surname, salary, phone, start_date, end_date 
    FROM {Variable.get('DATASET_ID')}.{landing_table} ) b on a.hash_id = b.hash_id 
    WHERE a.hash_id is null; 
"""

"""
INSERT INTO staging_dm_person
select b.hash_id, b.person_id, b.department_id, b.position_id, b.name, b.surname,
        b.salary, b.phone, PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.start_date), PARSE_DATETIME('%Y-%m-%d %H:%M:%S', b.end_date)
from dm_person a right join 
    (   select to_hex(md5(concat(department_id, position_id, name, surname, salary, phone, ifnull(end_date, '' )))) as hash_id, 
        person_id, department_id, position_id, name, surname, salary, phone, start_date, end_date
        from landing_dm_person ) b on a.hash_id = b.hash_id
where a.hash_id is null;
"""
/* After loading records, this switches the proper rows in the log to True */
BEGIN;

update van_people_loads.exdat_people_loadlog set loaded=True

where person_hash in (

    select person_hash from
    van_people_loads.new_{{ PPL }}_{{ STATE }}_{{ DATETIME_STR }}
    where person_hash is not null

);

COMMIT;

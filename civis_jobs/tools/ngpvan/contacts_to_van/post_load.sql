/* After loading records, this switches the proper rows in the log to True */
BEGIN;

update van_sq_loads.exdat_sq_loadlog set loaded=True

where sq_hash in (

    select sq_hash from
    van_sq_loads.new_{{ SQ }}_{{ STATE }}_{{ DATETIME_STR }}
    where sq_hash is not null

);

COMMIT;

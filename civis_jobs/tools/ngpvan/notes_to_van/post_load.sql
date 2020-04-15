/* After loading records, this switches the proper rows in the log to True */
BEGIN;

update van_note_loads.exdat_notes_loadlog set loaded=True

where note_hash in (

    select note_hash from
    van_note_loads.new_{{ NOTE }}_{{ DB }}_{{ STATE }}_{{ DATETIME_STR }}
    where note_hash is not null

);

COMMIT;

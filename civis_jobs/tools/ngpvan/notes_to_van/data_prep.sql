/* 1) create the set of new records to be added
   2) log the records with loaded as False
   3) create a view with only the columns the civis job needs
*/

BEGIN;

drop table if exists van_note_loads.new_{{ NOTE }}_{{ DB }}_{{ STATE }}_{{ DATETIME_STR }};
create table van_note_loads.new_{{ NOTE }}_{{ DB }}_{{ STATE }}_{{ DATETIME_STR }}
as (
    select

        van_id,
        "text",
        is_view_restricted,
        note_category_id,
        date_canvassed,
        contacttimestamp,
        state_code,
        note_hash

    from van_note_loads.{{ NOTE }}_{{ STATE }}

    where note_hash not in (
        select note_hash from van_note_loads.exdat_notes_loadlog
        where note_hash is not null
            and loaded is True)

);

COMMIT;


BEGIN;

insert into van_note_loads.exdat_notes_loadlog

(myc_van_id, "text", is_view_restricted,
 note_category_id, date_canvassed,
 contacttimestamp, state_code, note_hash, loaded)

    (select van_id::bigint as myc_van_id,
        "text",
        is_view_restricted,
        note_category_id,
        date_canvassed::datetime,
        contacttimestamp::datetime,
        state_code,
        note_hash,
        False as loaded

    from van_note_loads.new_{{ NOTE }}_{{ DB }}_{{ STATE }}_{{ DATETIME_STR }}

);

COMMIT;


BEGIN;

drop view if exists van_note_loads.{{ STATE }}_notes_current_load;
create view van_note_loads.{{ STATE }}_notes_current_load as (

    select van_id,
        "text",
        is_view_restricted,
        note_category_id,
        date_canvassed


    from van_note_loads.new_{{ NOTE }}_{{ DB }}_{{ STATE }}_{{ DATETIME_STR }}
);

COMMIT;

/* 1) create the set of new records to be added
   2) log the records with loaded as False
   3) create a view with only the columns the civis job needs 
*/
BEGIN;

drop table if exists van_sq_loads.new_{{ SQ }}_{{ STATE }}_{{ DATETIME_STR }};
create table van_sq_loads.new_{{ SQ }}_{{ STATE }}_{{ DATETIME_STR }}
as (
    select

        van_id,
        survey_question_id,
        survey_response_id,
        input_type_id,
        contact_type_id,
        date_canvassed,
        contacttimestamp,
        state_code,
        sq_hash

    from van_sq_loads.{{ SQ }}_{{ STATE }}
    where sq_hash not in (
        select sq_hash from van_sq_loads.exdat_sq_loadlog
        where sq_hash is not null
            and loaded is True)
);

COMMIT;

BEGIN;

insert into van_sq_loads.exdat_sq_loadlog
(myv_van_id, survey_question_id, survey_response_id, input_type_id,
contact_type_id, date_canvassed, contacttimestamp, state_code,
sq_hash, loaded)

    (select van_id::bigint as myv_van_id,
        survey_question_id::bigint,
        survey_response_id::bigint,
        input_type_id::bigint,
        contact_type_id::bigint,
        date_canvassed::datetime,
        contacttimestamp::datetime,
        state_code,
        sq_hash,
        False as loaded

    from van_sq_loads.new_{{ SQ }}_{{ STATE }}_{{ DATETIME_STR }}

);

COMMIT;

BEGIN;

drop view if exists van_sq_loads.{{ STATE }}_sqs_current_load;
create view van_sq_loads.{{ STATE }}_sqs_current_load as (

    select van_id,
        survey_question_id,
        survey_response_id,
        input_type_id,
        contact_type_id,
        date_canvassed

    from van_sq_loads.new_{{ SQ }}_{{ STATE }}_{{ DATETIME_STR }}

) with no schema binding;

COMMIT;

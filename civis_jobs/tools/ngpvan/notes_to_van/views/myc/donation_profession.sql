/* View for donation profession notes */
BEGIN;

create or replace view van_note_loads.donation_profession_{{ STATE }} as (

    select
        van_id
      , "text"
      , is_view_restricted
      , note_category_id
      , {{ TIMEZONE_SQL }}
      , contacttimestamp
      , donor_state as state_code
        -- hash of non null values needed for it to be unique
      , md5(van_id||"text"|| donor_state) as note_hash

    from (
        select distinct
            split_part(xwalk.st_myc_van_id, '-', 2) as van_id
          , 'Donor Occupation: ' || donor.occupation_clean || CHR(10) || 
            'Employer: ' || donor.employer_clean  as "text"
          , false is_view_restricted
          , {{ NOTE_ID }} as note_category_id
          , split_part(xwalk.st_myc_van_id, '-', 1) as donor_state
          , paidat as contacttimestamp
        from actblue.actblue_donor_basetable donor
        join bernie_data_commons.master_xwalk_ak xwalk using(actionkit_id)
        where donor_state = '{{ STATE }}'
    ) 
    where note_hash is not null

) with no schema binding;

COMMIT;

/* view pulling from CCC for will volunteer */
BEGIN;

create or replace view van_sq_loads.vol_ids_{{ STATE }} as (

    select

        van_id
      , survey_question_id
      , survey_response_id
      , input_type_id
      , contact_type_id
      , {{ TIMEZONE_SQL }}
      , contacttimestamp
      , state_code
        -- hash of non null values needed for it to be unique
      , md5(van_id||survey_question_id||
            survey_response_id||contact_type_id||
            contacttimestamp||state_code) as sq_hash

    from (

        select
            SPLIT_PART(ccc.st_myv_van_id, '-', 2) as van_id
          , 349878 as survey_question_id -- number for the IDs pipeline Q
      		-- case when for mapping strings to van response id values
            -- would need to be updated if more strings are added to contacts
          , case 
                when srt.surveyresponsetext = 'Volunteer Yes' then 1439463
                when srt.surveyresponsetext = 'Volunteer Maybe' then 1439464
        		when srt.surveyresponsetext = 'Volunteer No' then 1439465
            end as survey_response_id
          , 11 as input_type_id -- 11 is for API
          , case 
                when ccc.contacttype = 'getthru_dialer' then 19
                when ccc.contacttype = 'spoke' then 37
                when ccc.contacttype = 'bern_app_door_canvass' then 2
                when ccc.contacttype = 'bern_app_relational' then 131
                when ccc.contacttype = 'bern_app_crowd_canvass' then 139
                -- when ccc.contacttype = 'myc_phone' then 1
            end as contact_type_id
          , ccc.contacttimestamp
          , upper(SPLIT_PART(ccc.st_myv_van_id, '-', 1)) as state_code

        from contacts.contactscontact as ccc
        join contacts.surveyresponses as sr
            on ccc.contactcontact_id=sr.contactcontact_id
        join contacts.surveyresponsetext as srt
            on sr.surveyresponseid=srt.surveyresponseid

        where srt.surveyresponsetext in (
            'Volunteer Yes',
            'Volunteer No',
            'Volunteer Maybe')
            and upper(SPLIT_PART(ccc.st_myv_van_id, '-', 1)) = '{{ STATE }}'
            and st_myv_van_id is not null

    -- !!! NOTICE: EXCLUDING bern_app_door_canvass RESULTS
  ) where contact_type_id != 2 and sq_hash is not null

) with no schema binding;

COMMIT;

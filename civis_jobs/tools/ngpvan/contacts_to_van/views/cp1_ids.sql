/* view pulling from CCC for first choice candidate */
BEGIN;

create or replace view van_sq_loads.cp1_ids_{{ STATE }} as (

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
        -- notice! MyVoters ID not MyCampaign ID
        select

            SPLIT_PART(ccc.st_myv_van_id, '-', 2) as van_id
          , 349867 as survey_question_id -- number for the CP1 pipeline Q
            -- case when for mapping strings to van response id values
            -- would need to be updated if more strings are added to contacts
          , case 
                when srt.surveyresponsetext = 'Amy Klobuchar' then 1439386
                when srt.surveyresponsetext = 'Andrew Yang' then 1439371
                when srt.surveyresponsetext = 'Bernie Sanders' then 1439388
                when srt.surveyresponsetext = 'Beto' then 1439370
                when srt.surveyresponsetext = 'Bill de Blasio' then 1439389
                when srt.surveyresponsetext = 'Cory Booker' then 1439373
                when srt.surveyresponsetext = 'Donald Trump' then  1439365
                when srt.surveyresponsetext = 'Elizabeth Warren' then 1439364
                when srt.surveyresponsetext = 'Jay Inslee' then 1439385
                when srt.surveyresponsetext = 'Joe Biden' then 1439366
                when srt.surveyresponsetext = 'John Delaney' then 1439382
                when srt.surveyresponsetext = 'John Hickenlooper' then 1439384
                when srt.surveyresponsetext = 'Julian Castro' then 1439381
                when srt.surveyresponsetext = 'Kamala Harris' then 1439369
                when srt.surveyresponsetext = 'Kirsten Gillibrand' then 1439379
                when srt.surveyresponsetext = 'Marianne Williamson' then 1439376
                when srt.surveyresponsetext = 'Michael Bennet' then 1439391
                when srt.surveyresponsetext = 'Mike Gravel' then 1439375
                when srt.surveyresponsetext = 'Other' then 1439393
                when srt.surveyresponsetext = 'Pete Buttigieg' then 1439367
                when srt.surveyresponsetext = 'Steve Bullock' then 1439390
                when srt.surveyresponsetext = 'Tim Ryan' then 1439392
                when srt.surveyresponsetext = 'Tom Steyer' then 1439387
                when srt.surveyresponsetext = 'Tulsi Gabbard' then 1439368
            else null end as survey_response_id
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

        --- ccc is empty sometimes
        from contacts.contactscontact as ccc
        join contacts.surveyresponses as sr
            on ccc.contactcontact_id=sr.contactcontact_id
        join contacts.surveyquestiontext as sqt
            on sr.surveyquestionid=sqt.surveyquestionid
        join contacts.surveyresponsetext as srt
            on sr.surveyresponseid=srt.surveyresponseid

        -- Bern is 21, Getthru is 14
        where sqt.surveyquestiontext ilike ('%Candidate Interest First Choice%')
            and ccc.st_myv_van_id is not null
            and upper(SPLIT_PART(ccc.st_myv_van_id, '-', 1)) = '{{ STATE }}'
        -- !!! NOTICE: EXCLUDING bern_app_door_canvass RESULTS

    ) where contact_type_id != 2 and sq_hash is not null

) with no schema binding;

COMMIT;

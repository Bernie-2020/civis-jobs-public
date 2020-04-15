/* view pulling from CCJ for support ids */
BEGIN;

create or replace view van_sq_loads.myc_sup_ids_{{ STATE }} as (

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
            -- notice! MyVoters ID not MyCampaign ID
            SPLIT_PART(ccj.st_myc_van_id, '-', 2) as van_id
          , 349811 as survey_question_id -- number for the IDs pipeline Q
            -- case when for mapping strings to van response id values
            -- would need to be updated if more strings are added to contacts
          , case 
                when ccj.support_response_text = '1 - Strong Support' then 1439132
                when ccj.support_response_text = '2 - Will Likely Support Bernie' then 1439133
                when ccj.support_response_text = '3 - Undecided' then 1439134
                when ccj.support_response_text = '4 - Unlikely to Support Bernie' then 1439162
                when ccj.support_response_text = '5 - Will Not Support Bernie' then 1439163
                when ccj.support_response_text = '6 - Not Voting / Caucusing in the Democratic Primary' then 1439291
                when ccj.support_response_text = '7 - Dont know / Wont say' then 1439292
            end as survey_response_id

          , 11 as input_type_id -- 11 is for API
            -- mapping data sources to the agreed upon van contact types
          , case 
                when ccj.contacttype = 'getthru_dialer' then 19
                when ccj.contacttype = 'spoke' then 37
                when ccj.contacttype = 'bern_app_door_canvass' then 2
                when ccj.contacttype = 'bern_app_relational' then 131
                when ccj.contacttype = 'bern_app_crowd_canvass' then 139
                when ccj.contacttype = 'quickbase' then 145
                -- myc_phone should probably not be added but keeping it here
                -- when ccc.contacttype = 'myc_phone' then 1
            end as contact_type_id
          , ccj.contacttimestamp
          , upper(SPLIT_PART(ccj.st_myc_van_id, '-', 1)) as state_code

        from bernie_data_commons.contactcontacts_joined as ccj
        where upper(SPLIT_PART(ccj.st_myc_van_id, '-', 1)) = '{{ STATE }}'
            and ccj.st_myc_van_id is not null
            and ccj.st_myv_van_id is null --only run on myc ids that do not have myv
            and ccj.support_response_text is not null
            and ccj.support_response_text in (
                '1 - Strong Support',
                '2 - Will Likely Support Bernie',
                '3 - Undecided',
                '4 - Unlikely to Support Bernie',
                '5 - Will Not Support Bernie',
                '6 - Not Voting / Caucusing in the Democratic Primary',
                '7 - Dont know / Wont say'
                )

        -- !!! NOTICE: EXCLUDING bern_app_door_canvass RESULTS
      ) where contact_type_id != 2 and sq_hash is not null

) with no schema binding;

COMMIT;

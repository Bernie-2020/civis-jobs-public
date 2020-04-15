create or replace view van_people_loads.{{ PPL }}_{{ STATE }} as (
     select
        distinct first_name
      , middle_name
      , last_name
      , address_line_1
      , address_line_2
      , city
      , state_or_province
      , zip_or_postal_code
      , address_is_preferred
      , email
      , email_is_preferred
      , email_is_subscribed
      , email_subscription_status
      , home_phone
      , home_phone_is_preferred
      , home_phone_opt_in_status
      , cell_phone
      , cell_phone_is_preferred
      , cell_phone_opt_in_status
      , work_phone
      , work_phone_is_preferred
      , work_phone_opt_in_status
      , main_phone
      , main_phone_is_preferred
      , main_phone_opt_in_status
      , fax_phone
      , fax_phone_is_preferred
      , fax_phone_opt_in_status
      , external_id
      , external_id_type
      , civis_external_id
      , custom_field_id
      , md5(
            coalesce(first_name,'missing') ||
            coalesce(middle_name,'missing') ||
            coalesce(last_name,'missing') ||
            coalesce(address_line_1,'missing') ||
            coalesce(address_line_2,'missing') ||
            coalesce(city,'missing') ||
            coalesce(state_or_province,'missing') ||
            coalesce(zip_or_postal_code,'missing') ||
            coalesce(email,'missing') ||
            coalesce(home_phone,'missing') ||
            coalesce(cell_phone,'missing') ||
            coalesce(work_phone,'missing') ||
            coalesce(main_phone,'missing') ||
            coalesce(fax_phone,'missing') ||
            coalesce(civis_external_id,'missing') ||
            coalesce(custom_field_id,'missing') ||
            '{{ STATE }}' 
            ) as person_hash

    from (
        select
            myc.first_name
          , myc.middle_name
          , myc.last_name
          , ad.voting_address address_line_1
          , null address_line_2
          , ad.city
          , ad.state state_or_province
          , ad.zip  zip_or_postal_code
          , null address_is_preferred
          , lower(cem.email) email
          , cem.is_preferred_email email_is_preferred
          , null email_is_subscribed
          , null email_subscription_status
          , null home_phone
          , null home_phone_is_preferred
          , null home_phone_opt_in_status
          , null cell_phone
          , null cell_phone_is_preferred
          , null cell_phone_opt_in_status
          , null work_phone
          , null work_phone_is_preferred
          , null work_phone_opt_in_status
          , cpm.phone_number main_phone
          , cpm.is_preferred_phone main_phone_is_preferred
          , null main_phone_opt_in_status
          , null fax_phone
          , null fax_phone_is_preferred
          , null fax_phone_opt_in_status
          , null external_id
          , null external_id_type
            -- include the source state st_myc_van_id - does not go to van only to response table
          , es.state_code || '-' || es.myc_van_id civis_external_id
          , null custom_field_id
          , row_number() over(partition by myc.state_code, myc.myc_van_id) as rn
        from phoenix_demssanders20_vansync.event_signups es
        left join phoenix_demssanders20_vansync.person_records_myc myc
            on es.state_code=myc.state_code
            and es.myc_van_id=myc.myc_van_id
        left join phoenix_demssanders20_vansync.contacts_addresses_myc ad
            on myc.contacts_address_id = ad.contacts_address_id
            and ad.person_committee_id = 73296
            and myc.state_code = ad.state_code
            and myc.myc_van_id = ad.myc_van_id
        left join phoenix_demssanders20_vansync.contacts_emails_myc cem
            on cem.committee_id = 73296
            and myc.state_code = cem.state_code
            and myc.myc_van_id = cem.myc_van_id
            and myc.email_id = cem.contacts_email_id
            --and cem.is_preferred_email is true
        left join phoenix_demssanders20_vansync.contacts_phones_myc cpm
            on cpm.committee_id = 73296
            and myc.state_code = cpm.state_code
            and myc.myc_van_id = cpm.myc_van_id
            and myc.phone_id = cpm.contacts_phone_id
            --and cpm.is_preferred_phone is true

        left join phoenix_demssanders20_vansync.contacts_emails_myc cem2
            on lower(cem2.email) = lower(cem.email)
            and cem2.state_code = ad.state

        where es.created_by_committee_id=73296
            and es.state_code != ad.state
            and ad.state='{{ STATE }}'
            and cem2.myc_van_id is null --check that not already in home state

    )
    where rn=1
    limit 5
    )
;


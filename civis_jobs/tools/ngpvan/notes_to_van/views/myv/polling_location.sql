/* View for polling location notes */
BEGIN;


create or replace view van_note_loads.polling_location_{{ STATE }} as (

    select

        van_id
      , "text"
      , is_view_restricted
      , note_category_id
      , {{ TIMEZONE_SQL }}
      , state_code
        -- hash of non null values needed for it to be unique
      , md5(van_id||"text"|| state_code) as note_hash

    from (

        select
            myv_van_id as van_id
            -- inputting the canvassers email address into a note
          , [[[POLLING LOCATION TEXT HERE]]] as "text"
            /* is_view_restricted set to true if the note should be restricted
            only to certain users within the current context; set to false if
            the note may be viewed by any user in the current context. */
          , false is_view_restricted
          , {{ NOTE_ID }} as note_category_id
          , {{ STATE }} as state_code

        from [[[POLLING LOCATION TABLE]]]

      ) where note_hash is not null

) with no schema binding;

COMMIT;

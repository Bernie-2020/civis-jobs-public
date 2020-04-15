/* 1) create the set of new records to be added
   2) log the records with loaded as False
   3) create a view with only the columns the civis job needs
*/

BEGIN;

drop table if exists van_people_loads.new_{{ PPL }}_{{ STATE }}_{{ DATETIME_STR }};
create table van_people_loads.new_{{ PPL }}_{{ STATE }}_{{ DATETIME_STR }}
as (
    select
       first_name
        ,middle_name
        ,last_name
        ,address_line_1
        ,address_line_2
        ,city
        ,state_or_province
        ,zip_or_postal_code
        ,address_is_preferred
        ,email
        ,email_is_preferred
        ,email_is_subscribed
        ,email_subscription_status
        ,home_phone
        ,home_phone_is_preferred
        ,home_phone_opt_in_status
        ,cell_phone
        ,cell_phone_is_preferred
        ,cell_phone_opt_in_status
        ,work_phone
        ,work_phone_is_preferred
        ,work_phone_opt_in_status
        ,main_phone
        ,main_phone_is_preferred
        ,main_phone_opt_in_status
        ,fax_phone
        ,fax_phone_is_preferred
        ,fax_phone_opt_in_status
        ,external_id
        ,external_id_type
        ,civis_external_id
        ,custom_field_id
        ,person_hash


    from van_people_loads.{{ PPL }}_{{ STATE }}

    where person_hash not in (
        select person_hash from van_people_loads.exdat_people_loadlog
        where person_hash is not null
            and loaded is True)

);

COMMIT;


BEGIN;

insert into van_people_loads.exdat_people_loadlog


    (select
    first_name ,
    middle_name ,
    last_name ,
    address_line_1 ,
    address_line_2 ,
    city ,
	state_or_province ,
	email ,
    home_phone ,
    cell_phone ,
    main_phone ,
    civis_external_id ,
    person_hash ,
    False as loaded

    from van_people_loads.new_{{ PPL }}_{{ STATE }}_{{ DATETIME_STR }}

);

COMMIT;


BEGIN;

drop view if exists van_people_loads.{{ STATE }}_people_current_load;
create view van_people_loads.{{ STATE }}_people_current_load as (

    select first_name
        ,middle_name
        ,last_name
        ,address_line_1
        ,address_line_2
        ,city
        ,state_or_province
        ,zip_or_postal_code
        ,address_is_preferred
        ,email
        ,email_is_preferred
        ,email_is_subscribed
        ,email_subscription_status
        ,home_phone
        ,home_phone_is_preferred
        ,home_phone_opt_in_status
        ,cell_phone
        ,cell_phone_is_preferred
        ,cell_phone_opt_in_status
        ,work_phone
        ,work_phone_is_preferred
        ,work_phone_opt_in_status
        ,main_phone
        ,main_phone_is_preferred
        ,main_phone_opt_in_status
        ,fax_phone
        ,fax_phone_is_preferred
        ,fax_phone_opt_in_status
        ,external_id
        ,external_id_type
        ,civis_external_id
        ,custom_field_id

    from van_people_loads.new_{{ PPL }}_{{ STATE }}_{{ DATETIME_STR }}
);

COMMIT;

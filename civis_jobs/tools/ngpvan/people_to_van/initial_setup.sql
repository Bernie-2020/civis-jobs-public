/*
Log table for keeping track of what has been
attempted to be loaded into VAN SQs
*/

drop table if exists van_people_loads.exdat_people_loadlog;
create table van_people_loads.exdat_people_loadlog (

    first_name varchar(50) ENCODE zstd NULL,
    middle_name varchar(50) ENCODE zstd NULL,
    last_name varchar(50) ENCODE zstd NULL,
    address_line_1 varchar(50) ENCODE zstd NULL,
    address_line_2 varchar(50) ENCODE zstd NULL,
    city varchar(50) ENCODE zstd NULL,
	state_or_province varchar(10) ENCODE zstd NULL,
	email varchar(100) ENCODE zstd NULL,
    home_phone varchar(15) ENCODE zstd NULL,
    cell_phone varchar(15) ENCODE zstd NULL,
    main_phone varchar(15) ENCODE zstd NULL,
    civis_external_id varchar(25) encode zstd null,
    person_hash varchar(1024) ENCODE zstd NULL,
    loaded bool)
    distkey(person_hash)
    sortkey(person_hash);

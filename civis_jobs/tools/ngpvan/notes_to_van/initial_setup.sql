/*
Log table for keeping track of what has been
attempted to be loaded into VAN SQs
*/

drop table if exists van_note_loads.exdat_notes_loadlog;
create table van_note_loads.exdat_notes_loadlog (
    myc_van_id bigint,
    text varchar(200),
    is_view_restricted bool,
    note_category_id integer,
    contacttimestamp datetime,
    date_canvassed datetime,
	state_code varchar(10),
    note_hash varchar(1024),
    loaded bool)
    distkey(state_code)
    sortkey(note_hash, myc_van_id);

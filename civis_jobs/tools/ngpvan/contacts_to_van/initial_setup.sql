/*
Log table for keeping track of what has been
attempted to be loaded into VAN SQs
*/

drop table if exists van_sq_loads.exdat_sq_loadlog;
create table van_sq_loads.exdat_sq_loadlog (
    myv_van_id bigint,
  	survey_question_id bigint,
  	survey_response_id bigint,
  	input_type_id bigint,
    contact_type_id bigint,
  	date_canvassed datetime,
    contacttimestamp datetime,
	  state_code varchar(10),
    sq_hash varchar(1024),
    loaded bool)
    distkey(state_code)
    sortkey(sq_hash, myv_van_id);

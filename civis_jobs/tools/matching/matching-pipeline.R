# Container: https://platform.civisanalytics.com/spa/#/scripts/containers/67174246
# '/app/pipeline-etl/matching/match-pipeline-params.R'

config_path <- Sys.getenv('CONFIG_PATH')
source(config_path)

library(civis)
library(tidyverse)

# Alias column names
input_schema <- input_table_param$schema
input_table <- input_table_param$table

output_schema <- output_table_param$schema
output_table <- output_table_param$table

# Functions ---------------------------------------------------------------

dedupe_match_table <- function(input_schema_table = NULL,
                               match_schema_table = NULL,
                               output_schema_table = NULL,
                               extra_match = TRUE,
                               cutoff_param = 0) {
  sql_pii <- c()
  match_sql = ''
  extra_match_sort = ''
  for (i in names(compact(pii_param))) {
    v = paste0('\n,',compact(pii_param)[[i]],'')
    if (i == "first_name" && extra_match == TRUE) {
      first_name_match = paste0(' left(lower(input.',compact(pii_param)[[i]],'),3) ilike left(lower(first_phoenix),3) ')
    } 
    if (i == "last_name" && extra_match == TRUE) {
      last_name_match = paste0(' lower(input.',compact(pii_param)[[i]],') ilike lower(last_phoenix) ')
    } 
    if (i == "state_code" && extra_match == TRUE) { 
      state_code_match = paste0('  input.',compact(pii_param)[[i]]," is not null and input.",compact(pii_param)[[i]]," ilike phxp.state_code ")
    }
    sql_pii<- c(sql_pii,v)
  }
  if (all(c("first_name","last_name","state_code") %in% names(compact(pii_param))) && extra_match == TRUE) {
     match_sql = paste0("\n, case when ",state_code_match," and ",first_name_match," and ",last_name_match,' then 1 else 0 end as extra_match ')
     extra_match_sort = ' extra_match desc, '
  }
  sql_query_xwalk <- c()
  sql_query_xwalk <- paste0("(select person_id, voterbase_id, score , getdate()::date as matched_date ",paste0(sql_pii,collapse='')," from 
        (select * , row_number() over(partition by source_id order by ",extra_match_sort," score desc) as best_record_rank
        from (select phxp.person_id, match.source_id, match.matched_id as voterbase_id, match.score ",paste0(sql_pii,collapse=''),match_sql," from 
        (select * from ",match_schema_table," where matched_id is not null) match
        left join ",input_schema_table," input on match.source_id = input.",pii_param$primary_key,"
        left join (select person_id, voterbase_id, state_code, lower(first_name) as first_phoenix, lower(last_name) as last_phoenix from phoenix_analytics.person) phxp on match.matched_id = phxp.voterbase_id ))  
        where best_record_rank = 1 and (score >= ",cutoff_param," or extra_match = 1) and person_id is not null and voterbase_id is not null )")
  
  match_output_sql <- paste0('DROP TABLE IF EXISTS ',output_schema_table,"; 
                              CREATE TABLE ",output_schema_table,' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') AS ',sql_query_xwalk,';') 
  cat(match_output_sql,file="sql.sql")
  cat(match_output_sql)
  
  match_output_status <- civis::query_civis(x=sql(match_output_sql), database = 'Bernie 2020') 
  
  return(match_output_status)
}

# Drop staging tables if they exist ---------------------------------------

drop_tables_sql <- paste0('\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_0_input'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_1_match1'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_2_fullmatch'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_3_rematch'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_4_cass'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_6_match2'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_7_fullmatch'),';')
drop_table_status <- civis::query_civis(x=sql(drop_tables_sql), database = 'Bernie 2020')

# Setup Initial Table ----------------------------------------------------

# Check if final table exists and if so take records below rematch threshold from previous run
cass_row_count <- civis::read_civis(x=sql(paste0('select count(*) from ',input_schema,'.',input_table)), database = 'Bernie 2020')   
check_if_cass_table_exists <- civis::read_civis(x=sql(paste0("select count(*) from information_schema.tables where table_schema = '",output_schema,"' and table_name = '",paste0(input_table,'_stage_cass'),"';")), database = 'Bernie 2020')
if (check_if_cass_table_exists$count == 1) {
  cass_clean_row_count <- civis::read_civis(x=sql(paste0('select count(*) from ',output_schema,'.',input_table,'_stage_cass')), database = 'Bernie 2020')
  cass_row_count <- abs(cass_row_count - cass_clean_row_count)
}

if (ceiling((cass_row_count$count/250000)/5) > 3) {
        parallel_chunks <- 3
} else {
        parallel_chunks <- ceiling((cass_row_count$count/250000)/5)
}
check_if_final_table_exists <- civis::read_civis(x=sql(paste0("select count(*) from information_schema.tables where table_schema = '",output_schema,"' and table_name = '",output_table,"';")), database = 'Bernie 2020')
if (check_if_final_table_exists$count == 1) {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(input_schema,'.',input_table)," 
                                 left join (select ",pii_param$primary_key,", score from ",output_schema,'.',output_table,") using(",pii_param$primary_key,") where score < ",rematch_threshold," or score is null)")
} else {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(input_schema,'.',input_table),")")
}
# Create initial staging table based on above checks
input_table_sql <- paste0('drop table if exists ',paste0(output_schema,'.',input_table,'_stage_0_input'),';
                          create table ',paste0(output_schema,'.',input_table,'_stage_0_input'),
                          ' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',match_universe,';') 
input_table_status <- civis::query_civis(x=sql(input_table_sql), database = 'Bernie 2020') 

# Person Match  -----------------------------------------------------------

# Submit the person match job
match_job_civis <- civis::enhancements_post_civis_data_match(name = paste0('Civis Match Job 1: ',input_schema,'.',input_table),
                                                             input_field_mapping = compact(pii_param),
                                                             match_target_id = civis::match_targets_list()[[1]]$id, # Civis Voterfile = 1, DNC = 2
                                                             parent_id = NULL,
                                                             input_table = list(databaseName = 'Bernie 2020',
                                                                                schema = output_schema,
                                                                                table = paste0(input_table,'_stage_0_input')),
                                                             output_table = list(databaseName = 'Bernie 2020',
                                                                                 schema = output_schema,
                                                                                 table = paste0(input_table,'_stage_1_match1')),
                                                             max_matches = matches_per_id,
                                                             threshold = 0)
match_job_run_civis <- civis::enhancements_post_civis_data_match_runs(id = match_job_civis$id)

# Block until the match job finishes
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run_civis$civisDataMatchId,
           run_id=match_job_run_civis$id)
get_status(m)

# Best matches from first run
deduped_status <- dedupe_match_table(input_schema_table = paste0(output_schema,'.',input_table,'_stage_0_input'),
                                     match_schema_table = paste0(output_schema,'.',input_table,'_stage_1_match1'),
                                     output_schema_table = paste0(output_schema,'.',input_table,'_stage_2_fullmatch'),
                                     extra_match = use_extra_match,
                                     cutoff_param = cutoff_threshold)
deduped_status 

# CASS Address Standardization --------------------------------------------

if (enable_cass == TRUE) {  
        
        # Create table of below threshold matches to run through CASS and rematch again
        check_if_cass_table_exists <- civis::read_civis(x=sql(paste0("select count(*) from information_schema.tables where table_schema = '",output_schema,"' and table_name = '",paste0(input_table,'_stage_cass'),"';")), database = 'Bernie 2020')
        if (check_if_cass_table_exists$count == 1) {
                cass_universe <- paste0('(select input0.* from ',output_schema,'.',input_table,'_stage_0_input input0 
                                        left join 
                                        (select * from ',output_schema,'.',input_table,'_stage_2_fullmatch) input2 using(',pii_param$primary_key,') 
                                        left join 
                                        (select * from ',output_schema,'.',input_table,'_stage_cass) input3 using(',pii_param$primary_key,') 
                                        where input2.',pii_param$primary_key,' is null and input3.',pii_param$primary_key,' is null )')
        } else {
                cass_universe <- paste0('(select input0.* from ',output_schema,'.',input_table,'_stage_0_input input0 
                                        left join 
                                        (select * from ',output_schema,'.',input_table,'_stage_2_fullmatch) input2 using(',pii_param$primary_key,') 
                                        where input2.',pii_param$primary_key,' is null)')
        }
        rematch_table_sql <- paste0('create table ',output_schema,'.',input_table,'_stage_3_rematch as ',cass_universe,';')
        rematch_table_status <- civis::query_civis(x=sql(paste0(rematch_table_sql)), database = 'Bernie 2020') 
        rematch_table_status
        
        # Submit CASS jobs in parallel
        chunk_jobs <- c()
        chunk_runs <- c()
        for (chunk_i in 1:parallel_chunks) {
                Sys.sleep(2)
                print(chunk_i)
                clean_job <- civis::enhancements_post_cass_ncoa(name = paste0('CASS Job Chunk ',chunk_i,': ',input_schema,'.',input_table), 
                                                                source = list(databaseTable = list(schema = output_schema,
                                                                                                   table = paste0(input_table,'_stage_3_rematch'),
                                                                                                   remoteHostId = get_database_id('Bernie 2020'),
                                                                                                   credentialId = default_credential(),
                                                                                                   multipartKey = list(pii_param$primary_key))),
                                                                destination = list(databaseTable = list(schema = output_schema,
                                                                                                        table = paste0(input_table,'_stage_4_cass_',chunk_i))),
                                                                column_mapping = list(address1=pii_param$full_address,
                                                                                      address2=pii_param$unit,
                                                                                      city=pii_param$city,
                                                                                      state=pii_param$state_code,
                                                                                      zip=pii_param$zip#,name=paste0(pii_param$first_name,'+',pii_param$last_name)
                                                                ),
                                                                use_default_column_mapping = "false",
                                                                output_level = "cass",
                                                                limiting_sql = paste0(pii_param$primary_key,' is not null and chunk = ',chunk_i))
                clean_job_run <- enhancements_post_cass_ncoa_runs(clean_job$id)
                chunk_jobs <- c(chunk_jobs,clean_job_run$cassNcoaId)
                chunk_runs <- c(chunk_runs,clean_job_run$id)
        }
        
        rs <- await_all(f=enhancements_get_cass_ncoa_runs, .x = chunk_jobs, .y = chunk_runs)
        
        # Capture successful CASS jobs
        chunk_successes <- c()
        for (i in 1:parallel_chunks) {
                if (rs[[i]]['state'] == "succeeded") {
                        chunk_successes <- c(chunk_successes, i)
                        print('CASS Job Success: ',i)
                }
        }
        
        # Pull down all CASS tables that exist
        cass_tables_sql <- paste0("select tab.table_schema, tab.table_name, tinf.tbl_rows as table_rows from svv_tables tab join svv_table_info tinf on tab.table_schema = tinf.schema and tab.table_name = tinf.table where tab.table_schema = '", 
                                  output_schema,"' and tab.table_name similar to '%",input_table,"%' and tab.table_name similar to '%_stage_4_cass_%' and tab.table_schema not in('pg_catalog','information_schema') order by tinf.tbl_rows desc;")
        cass_tables_df <- read_civis(x = sql(cass_tables_sql), database = 'Bernie 2020')
        cass_tables_to_union <- cass_tables_df %>% filter(table_rows > 0) 
        
        # Union tables with more than 1 row (sometimes CASS jobs fail but successfully export a table)
        cass_union_sql <- c()
        cass_drop_sql <- c()
        for (cass_tbl in unique(cass_tables_to_union$table_name)) {
                u <- paste0("(select * from ",output_schema,'.',cass_tbl,')')
                cass_union_sql <- c(cass_union_sql, u)
        }
        cass_union_sql <- paste0('create table ',paste0(output_schema,'.',input_table,'_stage_4_cass'),' as (select * from ',paste(cass_union_sql, collapse = ' union all '),');')
        cat(cass_union_sql ,file="sql.sql")
        cass_union_status <- civis::query_civis(x=sql(cass_union_sql), database = 'Bernie 2020') 
        
        # Drop all intermediary chunked CASS tables
        for (cass_tbl in unique(cass_tables_df$table_name)) {
                d <- paste0("\n drop table if exists ",output_schema,'.',cass_tbl,'; ')
                cass_drop_sql <- c(cass_drop_sql, d)
        }
        cass_drop_sql <- paste(cass_drop_sql, collapse = ' ')
        cat(cass_drop_sql, file="sql.sql")
        cass_drop_status <- civis::query_civis(x=sql(cass_drop_sql), database = 'Bernie 2020') 
        
        # Coalesce CASS table with input table's PII
        coalesce_columns_sql <- c()
        for (i in names(compact(pii_param))) {
                v = paste0('input.',compact(pii_param)[[i]],'')
                if (i == "first_name") {
                        v = paste0("coalesce(name.first_name_guess, input.",compact(pii_param)[[i]],") as ",compact(pii_param)[[i]])
                }
                if (i == "last_name") {
                        v = paste0("coalesce(name.last_name_guess, input.",compact(pii_param)[[i]],") as ",compact(pii_param)[[i]])
                }
                if (i == "full_address") {
                        v = paste0('coalesce(cass.std_priadr::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
                }
                if (i == "unit") {
                        v = paste0('coalesce(cass.std_secadr::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
                }
                if (i == "city") {
                        v = paste0('coalesce(cass.std_city::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
                }
                if (i == "state_code") {
                        v = paste0('coalesce(cass.std_state::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
                }
                if (i == "zip") {
                        v = paste0('coalesce(cass.std_zip::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
                }
                coalesce_columns_sql <- c(coalesce_columns_sql,v)
        }
        
        # Guess first_name and last_name from email (or if last name is empty and two words are in first name field)
        if ((is.null(pii_param$first_name) && is.null(pii_param$last_name) && is.null(pii_param$email)) == FALSE) {
          clean_name_sql <- paste0(" left join (select ",pii_param$primary_key,", first_name_guess, last_name_guess 
                                                 from (select ",pii_param$primary_key,"
                                                ,nullif(initcap(SPLIT_PART(regexp_replace(lower(",pii_param$first_name,"),' and | & ',';'), ';', 1)),'') as first_name_partner_1
                                                ,nullif(initcap(SPLIT_PART(regexp_replace(lower(",pii_param$first_name,"),' and | & ',';'), ';', 2)),'') as first_name_partner_2
                                                ,nullif(regexp_replace(lower(LEFT(",pii_param$email,", nullif((CHARINDEX('@',",pii_param$email,") -1),-1) )), '[^a-zA-Z\\.\\_]',''),'') as email_preparse
                                                ,nullif(initcap(REGEXP_SUBSTR( email_preparse, '^([^._]+)')),'') as first_from_email
                                                ,nullif(initcap(REGEXP_SUBSTR( email_preparse, '[^._]*$')),'') as last_from_email
                                                ,nullif(initcap(left(",pii_param$first_name,", CHARINDEX(' ', ",pii_param$first_name,"))),'') as first_from_first
                                                ,nullif(initcap(substring(",pii_param$first_name,", CHARINDEX(' ', ",pii_param$first_name,")+1, len(",pii_param$first_name,")-(CHARINDEX(' ', ",pii_param$first_name,")-1))),'') as last_from_first
                                                ,case when last_from_first = ",pii_param$first_name," then NULL else last_from_first end as last_from_first_2
                                                ,case when (",pii_param$first_name," is not null and first_name_partner_2 is not null) and first_name_partner_1 <> '' then first_name_partner_1
                                                      when (",pii_param$first_name," is not null and ",pii_param$last_name," is null and last_from_first_2 is not null) and first_from_first <> '' then first_from_first
                                                      when (",pii_param$first_name," is null and last_from_email is not null) and (first_from_email <> last_from_email ) and first_from_email <> '' then first_from_email
                                                      else initcap(",pii_param$first_name,") end as first_name_guess
                                                ,case 
                                                      when ",pii_param$last_name," is null then nullif(coalesce(last_from_first_2, last_from_email),'') 
                                                      else initcap(",pii_param$last_name,") end as last_name_guess 
                                                      from (select *, row_number() over (partition by ",pii_param$primary_key," order by ",pii_param$email," nulls last) as rownum from ",output_schema,'.',input_table,'_stage_3_rematch) where rownum = 1)) name using(',pii_param$primary_key,') ')        
        } else {
          clean_name_sql <- paste0(" left join (select distinct ",pii_param$primary_key,", NULL as first_name_guess, NULL as last_name_guess from ",output_schema,'.',input_table,'_stage_3_rematch) name using(',pii_param$primary_key,') ')       
        }

        
        coalesce_table_sql <- paste0('\n (select ',paste(coalesce_columns_sql, collapse = '\n, '),' 
                                      \n from ',output_schema,'.',input_table,'_stage_3_rematch input',' 
                                      \n ',clean_name_sql,' 
                                      \n left join ',output_schema,'.',input_table,'_stage_4_cass cass',' 
                                      \n using(',pii_param$primary_key,') \nwhere ',pii_param$primary_key,' is not null)')
        
        # Submit coalesce query to combine raw PII and successful CASS jobs
        match_input_sql <- paste0('drop table if exists ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),';
                          create table ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',coalesce_table_sql,';')  
        cat(match_input_sql,file="sql.sql")
        match_input_status <- civis::query_civis(x=sql(match_input_sql), database = 'Bernie 2020') 
        match_input_status
        
        # Insert into exsiting or create new CASS results table
        if (check_if_cass_table_exists$count == 1) {
          cass_save_sql <- paste0('insert into  ',paste0(output_schema,'.',input_table,'_stage_cass'),' (select *, getdate()::date as cass_date from ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),');')
          cat(cass_save_sql ,file="sql.sql")
          cass_save_status <- civis::query_civis(x=sql(cass_save_sql), database = 'Bernie 2020') 
        } else {
          cass_save_sql <- paste0('create table ',paste0(output_schema,'.',input_table,'_stage_cass'),' as (select *, getdate()::date as cass_date from ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),');')
          cat(cass_save_sql ,file="sql.sql")
          cass_save_status <- civis::query_civis(x=sql(cass_save_sql), database = 'Bernie 2020') 
        }

        # Person Match --------------------------------------------------------
        
        # Submit the Person Match Job
        match_job_civis <- civis::enhancements_post_civis_data_match(name = paste0('Civis Match Job 2: ',input_schema,'.',input_table),
                                                                     input_field_mapping = compact(pii_param),
                                                                     match_target_id = civis::match_targets_list()[[1]]$id, # Civis Voterfile = 1, DNC = 2
                                                                     parent_id = NULL,
                                                                     input_table = list(databaseName = 'Bernie 2020',
                                                                                        schema = output_schema,
                                                                                        table = paste0(input_table,'_stage_5_coalesce')),
                                                                     output_table = list(databaseName = 'Bernie 2020',
                                                                                         schema = output_schema,
                                                                                         table = paste0(input_table,'_stage_6_match2')),
                                                                     max_matches = matches_per_id,
                                                                     threshold = 0)
        match_job_run_civis <- civis::enhancements_post_civis_data_match_runs(id = match_job_civis$id)
        
        # Block until the Match jobs finish
        m <- await(f=enhancements_get_civis_data_match_runs, 
                   id=match_job_run_civis$civisDataMatchId,
                   run_id=match_job_run_civis$id)
        get_status(m)
        
        # Find Best Records --------------------------------------------------------
        
        # Best matches from second run
        deduped_status <- dedupe_match_table(input_schema_table = paste0(output_schema,'.',input_table,'_stage_5_coalesce'),
                                             match_schema_table = paste0(output_schema,'.',input_table,'_stage_6_match2'),
                                             output_schema_table = paste0(output_schema,'.',input_table,'_stage_7_fullmatch'),
                                             extra_match = use_extra_match,
                                             cutoff_param = cutoff_threshold)
        deduped_status 
        
}

# Custom list of input columns
column_list <- paste0(' person_id , voterbase_id , score , matched_date , ',paste(as.vector(unlist(compact(pii_param))),collapse = ' , '))

# Union in existing matched output
if (check_if_final_table_exists$count == 1) {
        existing_universe <- paste0(" union all (select ",column_list," from ",output_schema,'.',output_table,")")
} else {
        existing_universe <- ''
}

# Union in rematched CASS results 
if (enable_cass == 1) {
        cass_rematch <- paste0(' union all (select * from ',output_schema,'.',input_table,'_stage_7_fullmatch)')
} else {
        cass_rematch <- ''
}

# Union together everything and select best matches
complete_table_sql <- paste0('drop table if exists ',output_schema,'.',output_table,'_all_matches; 
                              create table ',output_schema,'.',output_table,'_all_matches distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as 
                              (select ',column_list,' from
                              (select *, row_number() over(partition by ',pii_param$primary_key,' order by score desc) as best_record_rank from 
                              (select * from
                              (select * from ',output_schema,'.',input_table,'_stage_2_fullmatch) ',
                             cass_rematch, 
                             existing_universe, ')) where best_record_rank = 1);')

complete_table_status <- civis::query_civis(x=sql(complete_table_sql), database = 'Bernie 2020') 
complete_table_status

# Output matched table with only records above cutoff_threshold
final_table_sql <- paste0('drop table if exists ',output_schema,'.',output_table,'; 
                          create table ',output_schema,'.',output_table,' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as 
                          (select * from ',output_schema,'.',output_table,'_all_matches);')

final_table_status <- civis::query_civis(x=sql(final_table_sql), database = 'Bernie 2020') 
final_table_status

#Drop staging tables
drop_tables_sql <- paste0('\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_0_input'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_1_match1'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_2_fullmatch'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_3_rematch'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_4_cass'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_5_coalesce'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_6_match2'),';
\ndrop table if exists ',paste0(output_schema,'.',input_table,'_stage_7_fullmatch'),';')
drop_table_status <- civis::query_civis(x=sql(drop_tables_sql), database = 'Bernie 2020')

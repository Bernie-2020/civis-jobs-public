drop table if exists matching.ak_matched;
create table matching.ak_matched
(
    actionkit_id int ENCODE RAW DISTKEY NULL,
    voterbase_id varchar(100) ENCODE zstd NULL,
    score     float ENCODE zstd NULL,
    match_date date encode AZ64 null
)
DISTSTYLE KEY
sortkey(actionkit_id, voterbase_id);

grant select on matching.ak_matched to group bernie_data;

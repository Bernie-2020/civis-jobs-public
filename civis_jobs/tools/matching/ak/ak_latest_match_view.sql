

-- workflow: https://platform.civisanalytics.com/spa/#/workflows/10914
-- insert matched records into final table
insert into matching.ak_matched
    (actionkit_id, voterbase_id, score, match_date)

(select m.source_id::int actionkit_id
      , m.target_id as voterbase_id
      , m.score
      , getdate()::date from matching.ak_civis_match m
        left join matching.ak_matched am on m.source_id = am.actionkit_id
        where m.target_id is not null and am.actionkit_id is null);

drop table if exists matching.ak_matched_staging;
create table matching.ak_matched_staging sortkey(person_id) distkey(person_id) as
select actionkit_id, voterbase_id, score, match_date, person_id, ak_rn from
    (
        select m.actionkit_id
        ,m.voterbase_id
        ,m.score
        ,m.match_date
        , p.person_id
        , row_number() over(partition by p.person_id order by score desc, cu.updated_at desc) ak_rn
        , row_number() over(partition by m.actionkit_id, m.voterbase_id, m.score, m.match_date, p.person_id) rn
        from matching.ak_matched m
        left join phoenix_analytics.person p on m.voterbase_id = p.voterbase_id and p.reg_voter_flag is true
        join ak_bernie.core_user cu on m.actionkit_id = cu.id
    )
    where rn = 1;

drop table matching.ak_matched;
alter table matching.ak_matched_staging rename to ak_matched;


grant select on matching.ak_matched to group bernie_data, group haystaq;



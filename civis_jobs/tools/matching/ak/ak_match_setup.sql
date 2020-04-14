
--workflow: https://platform.civisanalytics.com/spa/#/workflows/10914
drop table if exists matching.ak_for_matching;
create table matching.ak_for_matching as (
    with phones as (select *, row_number() over(partition by user_id order by cp.updated_at desc) phone_rn from ak_bernie.core_phone cp)

    select cu.*
         , ph1.normalized_phone as phone_1
        , ph2.normalized_phone as phone_2
        from ak_bernie.core_user cu
        left join phones ph1 on cu.id = ph1.user_id and ph1.phone_rn = 1
        left join phones ph2 on cu.id = ph2.user_id and ph2.phone_rn = 2

    left join matching.ak_matched m on cu.id = m.actionkit_id
    where m.actionkit_id is null
    );
    
    drop table if exists matching.ak_civis_match;


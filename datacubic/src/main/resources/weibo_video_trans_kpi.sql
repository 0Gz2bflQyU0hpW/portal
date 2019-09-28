select "fulllink-weibo-video-trans-kpi" business,nid,kpi from (
select kpi,row_number() OVER (ORDER BY kpi) nid from(
    select cast(file_duration as float) / cast(sinatrans_duration as float) kpi
    from tab_summary
    where fast = 'true'
    and (object_id like '1034%' OR object_id like '2358773%')
    and post_vbitrate > 0
    and label = 'mp4_hd'
) a0 ) b0
where nid <= 100
union all
select "fulllink-weibo-video-trans-kpi" business,"avg" nid, avg(kpi) kpi from
(select cast(file_duration as float) / cast(sinatrans_duration as float) kpi
    from tab_summary
    where fast = 'true'
    and (object_id like '1034%' OR object_id like '2358773%')
    and post_vbitrate > 0
    and label = 'mp4_hd') a1
union all
select "fulllink-weibo-video-trans-kpi" business,"max" nid, max(kpi) kpi from
(select cast(file_duration as float) / cast(sinatrans_duration as float) kpi
    from tab_summary
    where fast = 'true'
    and (object_id like '1034%' OR object_id like '2358773%')
    and post_vbitrate > 0
    and label = 'mp4_hd') a2
select "video-display-top" business,
  oid,
  sum(if(valid_play_duration> 300 ,1,0)) valid_pv
from(
  select oid,cast(valid_play_duration as bigint) valid_play_duration
       from video_raw
  where oid like '1034%' or oid like '2017607%'
  ) raw
group by oid
order by valid_pv desc
limit 10000
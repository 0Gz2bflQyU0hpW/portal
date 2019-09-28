select b00.dis_name,
 count(1) coun,
 count(distinct a0.video_id) diss_coun,
 sum(if(b00.vid is not null, 1, 0)) succ,
 sum(if(sn > 1,1,0)) error
from (select distinct video_id
 from video_raw) a0 left outer join
(select split(videos,";")[0] vid,
   split(split(videos,";")[1],',')[0] dis_name,
   count(1) sn
 from (
   select videos
   from cfg_resource
   lateral view explode(split(logs,'\\|')) id1 as videos
 ) b0
 group by split(videos,";")[0],
   split(split(videos,";")[1],',')[0]
) b00
on a0.video_id = b00.vid
group by b00.dis_name
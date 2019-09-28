select count(1) a1,sum(numb) a2 from(
select video_mediaid,sum(nu) numb
from source_table
group by video_mediaid) c00
select cast(_timestamp as bigint) timestamp,
 "dip-video-library" business,
 uicode,
 oid,
 sum(if(playduration > 0 ,1,0)) numb,
 sum(if(valid_play_duration > 300 ,1,0)) valid_numb,
 cast(sum(if(playduration > 0 ,playduration,0)) as bigint) playduration,
 cast(sum(if(valid_play_duration > 300 ,valid_play_duration,0)) as bigint) valid_play_duration from
    (select uid,
        uicode,
        cast(valid_play_duration as float) valid_play_duration,
        cast(playduration as float) playduration,
        oid
     from video_raw) raw
group by uicode,
 oid
order by numb desc
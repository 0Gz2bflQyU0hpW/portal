select job_create_time,
    business,
    object_id,
    client,
    prev_vbitrate,
    prev_resolution,
    source_file_duration,
    sum(source_file_duration_sum) source_file_duration_sum,
    count(1) unique_total_num
from(
    select job_create_time,
        "fulllink-weibo-video-trans-input" business,
        split(object_id, ':')[0] object_id,
        client,
        case when prev_vbitrate < 500 then '0-500'
                    when prev_vbitrate >= 500 and prev_vbitrate < 1000 then '500k-1m'
                    when prev_vbitrate >= 1000 and prev_vbitrate < 2000 then '1m-2m'
                    when prev_vbitrate >= 2000 and prev_vbitrate < 3000 then '2m-3m'
                    when prev_vbitrate >= 3000 and prev_vbitrate < 5000 then '3m-5m'
                    when prev_vbitrate >= 5000 and prev_vbitrate < 10000 then '5m-10m'
                    when prev_vbitrate >= 10000  then '10m-'
                    else '0'
        end prev_vbitrate,
        case when prev_width > 0 and prev_height > 0 and (prev_width < 240 or prev_height < 240) then '0-240p'
                    when prev_width >= 240 and prev_height >= 240 and (prev_width < 360 or prev_height < 360) then '240-360p'
                    when prev_width >= 360 and prev_height >= 360 and (prev_width < 480 or prev_height < 480) then '360-480p'
                    when prev_width >= 480 and prev_height >= 480 and (prev_width < 720 or prev_height < 720) then '480-720p'
                    when prev_width >= 720 and prev_height >= 720 and (prev_width < 1080 or prev_height < 1080) then '720-1080p'
                    when prev_width >= 1080 and prev_height >= 1080 then '1080p-'
                    else '0'
        end prev_resolution,
        case when source_file_duration >= 0 and source_file_duration < 5 then '0-5'
                    when source_file_duration >= 5 and source_file_duration < 15 then '5-15'
                    when source_file_duration >= 15 and source_file_duration < 30 then '15-30'
                    when source_file_duration >= 30 and source_file_duration < 60 then '30-60'
                    when source_file_duration >= 60 and source_file_duration < 120 then '60-120'
                    when source_file_duration >= 120 and source_file_duration < 180 then '120-180'
                    when source_file_duration >= 180  and source_file_duration < 300 then '180-300'
                    when source_file_duration >= 300  and source_file_duration < 900 then '300-900'
                    else 'other'
        end source_file_duration,
        cast(source_file_duration as float) source_file_duration_sum
    from tab_summary
    where (object_id like '1034%' OR object_id like '2358773%')
    AND post_vbitrate > 0
    and fast = 'true'
    AND label = 'mp4_hd'
) b0
group by
    job_create_time,
    business,
    object_id,
    client,
    prev_vbitrate,
    prev_resolution,
    source_file_duration
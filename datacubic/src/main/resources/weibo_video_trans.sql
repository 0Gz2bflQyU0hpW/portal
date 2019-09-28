select object_id,
    business,
    client,
    fast,
    label,
    output_vcodec,
    error_code,
    prev_vbitrate,
    prev_resolution,
    post_vbitrate,
    sum(file_duration) file_duration,
    sum(post_file_bitrate) post_file_bitrate,
    sum(sinatrans_duration) sinatrans_duration,
    count(1) total_num
from(
    select split(object_id, ':')[0] object_id,
        "fulllink-weibo-video-trans-total" business,
        client,
        fast,
        label,
        split(label, '_')[0] output_vcodec,
        cast(error_code as bigint) error_code,
        case when prev_vbitrate < 500 then '0-500k'
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
        case when post_vbitrate < 100 then '0-100'
            when post_vbitrate >= 100 and post_vbitrate < 200 then '100-200'
            when post_vbitrate >= 200 and post_vbitrate < 300 then '200-300'
            when post_vbitrate >= 300 and post_vbitrate < 400 then '300-400'
            when post_vbitrate >= 400 and post_vbitrate < 600 then '400-600'
            when post_vbitrate >= 600 and post_vbitrate < 800 then '600-800'
            when post_vbitrate >= 800 and post_vbitrate < 1000 then '800-1000'
            when post_vbitrate >= 1000 and post_vbitrate < 2600 then '1000-2600'
            when post_vbitrate >= 2600 and post_vbitrate < 10000 then '2600-10000'
            when post_vbitrate >= 1000 then '10000-'
            else '0'
        end post_vbitrate,
        cast(file_duration as float) file_duration,
        cast(post_file_bitrate as float) post_file_bitrate,
        cast(sinatrans_duration as float) sinatrans_duration
    from tab_summary
    where (object_id like '1034%' OR object_id like '2358773%')
    AND post_vbitrate > 0
    AND label in ('mp4_ld', 'mp4_hd', 'mp4_720p', 'mp4_1080p', 'hevc_mp4_hd', 'hevc_mp4_ld', 'hevc_mp4_720p')
) b0
group by
    object_id,
    business,
    client,
    fast,
    label,
    output_vcodec,
    error_code,
    prev_vbitrate,
    prev_resolution,
    post_vbitrate
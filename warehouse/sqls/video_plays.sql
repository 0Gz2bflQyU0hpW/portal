set mapreduce.job.queuename=back5;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set yarn.app.mapreduce.am.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;
set mapreduce.map.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;
set mapreduce.reduce.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;

INSERT INTO TABLE
    video_warehouse.video_plays
PARTITION
    (
        wtime
    )
SELECT
    platform,
    videoid,
    sum(play) AS plays,
    wtime
FROM
    (
        SELECT
            platform_video[0] AS platform,
            platform_video[1] AS videoid,
            1 AS play,
            wtime
        FROM
            (
                SELECT
                    split(oid, ':') AS platform_video,
                    functions.parse_mobileaction_extra(extra)['valid_play_duration'] AS valid_play_duration,
                    date_format(logtime, 'yyyyMMddHH') AS wtime
                FROM
                    warehouse.mobileaction799
                WHERE
                    dirtime = functions.to_dirtime($SCHEDULETIME, -1)
                    AND oid IS NOT NULL
            ) source_table
        WHERE
            size(platform_video) = 2
            AND valid_play_duration IS NOT NULL
            AND cast(valid_play_duration AS int) > 300
            AND wtime IS NOT NULL
    ) middle_table
GROUP BY
    platform,
    videoid,
    wtime;
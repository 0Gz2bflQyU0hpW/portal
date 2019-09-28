set mapreduce.job.queuename=back5;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set yarn.app.mapreduce.am.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;
set mapreduce.map.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;
set mapreduce.reduce.env=yarn_nodemanager_docker_container_executor_image_name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2,yarn_nodemanager_docker_container_extra_dir=/data0/dipplat/software/systemfile/iplibrary;

INSERT INTO TABLE
    video_warehouse.video_plays_distribution
PARTITION
    (
        wtime
    )
SELECT
    country,
    province,
    city,
    district,
    isp,
    sum(play) as plays,
    wtime
FROM
    (
        SELECT
            location['country'] AS country,
            location['province'] AS province,
            location['city'] AS city,
            location['district'] AS district,
            location['isp'] AS isp,
            1 AS play,
            wtime
        FROM
            (
                SELECT
                    functions.ip_to_location(ip) AS location,
                    date_format(logtime, 'yyyyMMddHH') AS wtime
                FROM
                    warehouse.mobileaction799
                WHERE
                    dirtime = functions.to_dirtime($SCHEDULETIME, -1)
                    AND oid IS NOT NULL
                    AND ip IS NOT NULL
            ) source_table
    )   middle_table
GROUP BY
    country,
    province,
    city,
    district,
    isp,
    wtime;
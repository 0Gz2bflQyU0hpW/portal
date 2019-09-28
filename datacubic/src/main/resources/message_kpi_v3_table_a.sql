SELECT sub_type,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       size_limit,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num,
       sum(during_time) - sum(send_during_time) AS during_time,
       sum(size_speed) AS size_speed
FROM (SELECT  sub_type,
        uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    net,
		    iplocations["isp"] AS isp,
		    iplocations["province"] AS province,
		  	CASE WHEN (sub_type = "upload_image" OR sub_type = "download_image") AND (size / 1024) < 1 THEN "small"
		        WHEN (sub_type = "upload_image" OR sub_type = "download_image") AND (size / 1024) > 1 THEN "large"
		        WHEN sub_type = "upload_video" AND (size / 1024) < 5 THEN "small"
		        WHEN sub_type = "upload_video"  AND (size / 1024) > 5 THEN "large"
		     END AS size_limit,
		    total_num,
		    succeed_num,
        during_time,
        send_during_time,
        CASE WHEN requestStatus = "success" THEN size / during_time
             ELSE 0.00
        END AS size_speed,
		    size
FROM (SELECT
    CASE WHEN (HEX(SUBSTR(`from`, 3, 3)) < HEX("872") AND action = "msg_performance" AND name = "attachment/upload" AND type = 1)
              OR
              (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872") AND
              ((action = "msg_performance" AND name = "sendPicMsg") OR
              ((action = "msg" OR action = "msg_failure") AND (proto_type = "2,0" OR proto_type = "2,5" OR proto_type = "6,0" OR proto_type = "6,11")))) THEN "upload_image"
         WHEN (HEX(SUBSTR(`from`, 3, 3)) < HEX("872") AND action = "msg_performance" AND name = "attachment/upload" AND type = 135)
              OR
              (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872") AND
              ((action = "msg_performance" AND name = "sendVideoMsg") OR
              ((action = "msg" OR action = "msg_failure") AND (proto_type = "2,0" OR proto_type = "2,5" OR proto_type = "6,0" OR proto_type = "6,11")))) THEN "upload_video"
         WHEN action = "msg_performance" AND name = "http://upload.api.weibo.com/2/mss/msget" AND type = 1 THEN "download_image"
         WHEN action = "msg_performance" AND name = "/2/mss/msget" AND type = 2 THEN "download_audio"
    END AS sub_type,
    parseUAInfo(ua) AS uainfos,
    ap AS net,
    ipToLocation(cip) AS iplocations,
    1 AS total_num,
    CASE WHEN (requestStatus = "success" AND action = "msg_performance") OR action = "msg" THEN 1
         ELSE 0
    END AS succeed_num,
    CASE WHEN requestStatus = "success" AND action = "msg_performance" THEN CAST((endTime - start) AS FLOAT ) / 1000000
         ELSE 0.00
    END AS during_time,
    CASE WHEN  action = "msg"  THEN CAST((endTime - start) AS FLOAT ) / 1000000
         ELSE 0.00
    END AS send_during_time,
    CASE WHEN (requestStatus = "success" AND action = "msg_performance") OR action = "msg"  THEN  CAST(size AS FLOAT) / 1024
         ELSE 0.00
    END AS size,
    requestStatus
    FROM
    message_kpi
    WHERE isSampled != "0"
    ) temp
    WHERE during_time != 0
  ) temp1
    WHERE sub_type IS NOT NULL AND size / 1024 <= 50 AND during_time / 60 >= 0 AND during_time / 60 < 10
    GROUP BY sub_type, system, app_version, system_version, net, isp, province, size_limit











SELECT send_type,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num,
       sum(during_time) AS during_time
FROM (SELECT  send_type,
        uainfos["app_version"] AS app_version,
        uainfos["system"] AS system,
        uainfos["system_version"] AS system_version,
        net,
        iplocations["isp"] AS isp,
        iplocations["province"] AS province,
        total_num,
        succeed_num,
        during_time,
        size
FROM (SELECT
    CASE WHEN (proto_type = "2,0" OR proto_type = "6,0") AND type = 0 THEN "send_text"
         WHEN (HEX(SUBSTR(`from`, 3, 3)) < HEX("872") AND (proto_type = "2,0" OR proto_type = "6,0") AND type = 1)
              OR
              (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872") AND action = "msg_performance" and  name = "sendPicMsg") THEN "send_image"
         WHEN (HEX(SUBSTR(`from`, 3, 3)) < HEX("872") AND (proto_type = "2,0" OR proto_type = "6,0") AND type = 2)
              OR
              (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872") AND action = "msg_performance" and  name = "sendAudioMsg") THEN "send_audio"
         WHEN (HEX(SUBSTR(`from`, 3, 3)) < HEX("872") AND (proto_type = "2,0" OR proto_type = "6,0") AND type = 135)
              OR
              (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872") AND action = "msg_performance" and  name = "sendVideoMsg") THEN "send_video"
    END AS send_type,
    parseUAInfo(ua) AS uainfos,
    ap AS net,
    ipToLocation(cip) AS iplocations,
    1 AS total_num,
    CASE WHEN action = "msg" OR (action = "msg_performance" AND requestStatus = "success") THEN 1
         ELSE 0
    END AS succeed_num,
    CASE WHEN action = "msg" OR (action = "msg_performance" AND requestStatus = "success")  THEN CAST(r AS FLOAT)  / 1000
         ELSE 0.00
    END AS during_time,
    CAST(size AS FLOAT) / 1024 AS size
    FROM
    message_kpi
    WHERE isSampled != "0"
    ) temp
  ) temp1
    WHERE send_type IS NOT NULL AND size / 1024 <= 50 AND during_time / 60 <= 10
    GROUP BY send_type, system, app_version, system_version, net, isp, province











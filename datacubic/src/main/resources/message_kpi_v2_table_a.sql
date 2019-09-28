SELECT request_url,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num,
       sum(stallTimes) AS stallTimes,
       sum(0_stallTimes) AS 0_stallTimes
FROM (SELECT  request_url,
        uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    net,
		    iplocations["isp"] AS isp,
		    iplocations["province"] AS province,
		    total_num,
        succeed_num,
        stallTimes,
        0_stallTimes
FROM (SELECT
    CASE WHEN action = "msg_performance" THEN "play_video"
    END AS request_url,
    parseUAInfo(ua) AS uainfos,
    ap AS net,
    ipToLocation(cip) AS iplocations,
    1 AS total_num,
    CASE WHEN action = "msg_performance" AND (requestStatus = "success" OR error = "N/A")THEN 1
         ELSE 0
    END AS succeed_num,
    CAST (stallTimes AS BIGINT) stallTimes,
    CASE WHEN stallTimes = 0 THEN 1
         ELSE 0
    END AS 0_stallTimes
    FROM
    message_kpi
    WHERE firstFrame IS NOT NULL AND isSampled != "0"
    ) temp
  ) temp1
    WHERE request_url IS NOT NULL
    GROUP BY request_url, system, app_version, system_version, net, isp, province











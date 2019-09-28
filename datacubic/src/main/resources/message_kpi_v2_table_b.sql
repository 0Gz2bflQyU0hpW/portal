SELECT request_url,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       sum(total_num) AS total_num,
       sum(during_time) AS during_time,
       sum(during_time_1s) AS during_time_1s,
       sum(during_time_2s) AS during_time_2s,
       sum(during_time_3s) AS during_time_3s,
       sum(during_time_4s) AS during_time_4s,
       sum(during_time_5s) AS during_time_5s,
       sum(during_time_long) AS during_time_long
FROM (SELECT  request_url,
        uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    net,
		    iplocations["isp"] AS isp,
		    iplocations["province"] AS province,
		    total_num,
        during_time,
		    CASE
		 	     WHEN during_time > 0 AND during_time <= 1 THEN 1
		 	     ELSE 0
		 	  END AS during_time_1s,
		 	  CASE
		 	     WHEN during_time > 1 AND during_time <= 2 THEN 1
		 	     ELSE 0
		 	  END AS during_time_2s,
		 	  CASE
		 	     WHEN during_time > 2 AND during_time <= 3 THEN 1
		 	     ELSE 0
		 	  END AS during_time_3s,
		 	  CASE
		 	     WHEN during_time > 3 AND during_time <= 4 THEN 1
		 	     ELSE 0
		 	  END AS during_time_4s,
		 	  CASE
		 	     WHEN during_time > 4 AND during_time <= 5 THEN 1
		 	     ELSE 0
		 	  END AS during_time_5s,
		 	  CASE
		 	     WHEN during_time > 5  THEN 1
		 	     ELSE 0
		 	  END AS during_time_long
FROM (SELECT
    CASE WHEN action = "msg_performance" THEN "play_video"
    END AS request_url,
    parseUAInfo(ua) AS uainfos,
    ap AS net,
    ipToLocation(cip) AS iplocations,
    1 AS total_num,
    CAST((firstFrame - start) AS FLOAT ) / 1000000  AS during_time
    FROM
    message_kpi
    WHERE firstFrame IS NOT NULL
    AND isSampled != "0"
    ) temp
    WHERE during_time > 0 AND during_time < 60
  ) temp1
    WHERE request_url IS NOT NULL
    GROUP BY request_url, system, app_version, system_version, net, isp, province











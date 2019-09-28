SELECT request_url,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       sum(total_num) AS total_num,
       sum(dl) as dl,
	     sum(sc) as sc,
	     sum(ssc) as ssc,
	     sum(sr) as sr,
	     sum(ws) as ws,
	     sum(rb) as rb
FROM (SELECT  request_url,
        uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    net,
		    iplocations["isp"] AS isp,
		    iplocations["province"] AS province,
		    total_num,
		 	  dl,
	      sc,
			  ssc,
		    sr,
			  ws,
			  rb
FROM (SELECT
   CASE WHEN action = "msg_performance" AND name = "/2/direct_messages/user_list" THEN "/2/direct_messages/user_list"
        WHEN action = "msg_performance" AND name = "/2/messageflow/notice" THEN "/2/messageflow/notice"
        WHEN action = "msg_performance" AND name = "/2/statuses/mentions" THEN "/2/statuses/mentions"
        WHEN action = "msg_performance" AND name = "/2/comments/to_me" THEN "/2/comments/to_me"
        WHEN action = "msg_performance" AND name = "/2/like/to_me" THEN "/2/like/to_me"
        WHEN action = "msg_performance" AND name = "/2/comments/mentions" THEN "/2/comments/mentions"
        WHEN action = "msg_performance" AND name = "/2/page/get_search_suggest" THEN "/2/page/get_search_suggest"
        WHEN (action = "msg" OR action = "msg_failure") AND proto_type = "5,35"  THEN "open_directmessage_dialog"
        WHEN (action = "msg" OR action = "msg_failure") AND proto_type = "6,34"  THEN "open_groupchat_dialog"
        WHEN (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872")
             AND (action = "msg" OR action = "msg_failure" OR action = "msg_performance")
             AND (proto_type = "2,0" OR proto_type = "2,5" OR name = "sendAudioMsg")
             AND (type = 0 OR type = 1 OR type = 135 OR class = 0))
              OR
             (HEX(SUBSTR(`from`, 3, 3)) < HEX("872")
             AND (action = "msg" OR action = "msg_failure")
             AND (proto_type = "2,0" OR proto_type = "2,5")) THEN "send_directmessage"
        WHEN (HEX(SUBSTR(`from`, 3, 3)) >= HEX("872")
             AND (action = "msg" OR action = "msg_failure" OR action = "msg_performance")
             AND (proto_type = "6,0" OR proto_type = "6,11" OR name = "sendAudioMsg")
             AND (type = 0 OR type = 1 OR type = 135 OR class = 2))
              OR
             (HEX(SUBSTR(`from`, 3, 3)) < HEX("872")
             AND(action = "msg" OR action = "msg_failure")
             AND (proto_type = "6,0" OR proto_type = "6,11"))THEN "send_groupchat_message"
        WHEN (action = "msg" OR action = "msg_failure") AND proto_type = "5,37"  THEN "open_subscribe_message_list"
    END AS request_url,
    parseUAInfo(ua) AS uainfos,
    ap AS net,
    ipToLocation(cip) AS iplocations,
    1 AS total_num,
    CAST(dl AS FLOAT ) AS dl,
    CAST((sc - ssc ) AS FLOAT) AS sc,
    CAST(ssc AS FLOAT) AS ssc,
    CAST(sr AS FLOAT) AS sr,
    CAST(ws AS FLOAT) AS ws,
    CAST(rb AS FLOAT) AS rb
    FROM
    message_kpi
    WHERE dl IS NOT NULL
					AND sc IS NOT NULL
					AND ssc IS NOT NULL
					AND sr IS NOT NULL
					AND ws IS NOT NULL
					AND rb IS NOT NULL
					AND isSampled != "0"
    ) temp
  ) temp1
    WHERE request_url IS NOT NULL
    AND dl >= 0 AND dl < 10000
    AND sc >= 0 AND sc < 10000
	  AND ssc >= 0 AND ssc < 10000
	  AND sr >= 0 AND sr < 10000
	  AND ws >= 0 AND ws < 10000
	  AND rb >= 0 AND rb < 10000
    GROUP BY request_url, system, app_version, system_version, net, isp, province











SELECT request_url,
       system,
       app_version,
       system_version,
       net,
       isp,
       province,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num
FROM (SELECT  request_url,
        uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    net,
		    iplocations["isp"] AS isp,
		    iplocations["province"] AS province,
		    total_num,
		    succeed_num
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
    CASE WHEN action = "msg_performance" AND requestStatus = "success" THEN 1
         WHEN action = "msg" THEN 1
         ELSE 0
    END AS succeed_num
    FROM
    message_kpi
    WHERE isSampled != "0"
    ) temp
  ) temp1
    WHERE request_url IS NOT NULL
    GROUP BY request_url, system, app_version, system_version, net, isp, province











SELECT request_url,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num,
       sum(request_time) AS request_time,
       sum(request_num) AS request_num
FROM (SELECT
    CASE WHEN regexp_extract(request,'/2/direct_messages/contacts.json',0) = "/2/direct_messages/contacts.json" THEN "/2/direct_messages/user_list"
        WHEN regexp_extract(request,'/2/doubleflow/messages.json',0) = "/2/doubleflow/messages.json" THEN "/2/messageflow/notice"
        WHEN regexp_extract(request,'/2/statuses/mentions.json',0) = "/2/statuses/mentions.json" THEN "/2/statuses/mentions"
        WHEN regexp_extract(request,'/2/comments/to_me.json',0) = "/2/comments/to_me.json" THEN "/2/comments/to_me"
        WHEN regexp_extract(request,'/2/attitudes/to_me.json',0) = "/2/attitudes/to_me.json" THEN "/2/like/to_me"
        WHEN regexp_extract(request,'/2/comments/mentions.json',0) = "/2/comments/mentions.json" THEN "/2/comments/mentions.json"
    END AS request_url,
    1 AS total_num,
    CASE WHEN status = 200 THEN 1
         ELSE 0
    END  AS succeed_num,
    CAST(requestTime AS FLOAT) * 1000  AS request_time,
    CASE WHEN status != 200 OR requestTime < 0 OR requestTime > 120 THEN 0
         ELSE 1
    END AS request_num
    FROM
    message_kpi
    ) temp
    WHERE request_url IS NOT NULL
    GROUP BY request_url










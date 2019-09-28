SELECT request_url,
       sum(total_num) AS total_num,
       sum(succeed_num) AS succeed_num,
       sum(request_time) AS request_time
FROM (SELECT
   CASE WHEN name = "(5,35)"  THEN "open_directmessage_dialog"
        WHEN name = "(6,34)"  THEN "open_groupchat_dialog"
        WHEN name = "(2,0)" OR name = "(2,5)" THEN "send_directmessage"
        WHEN name = "(6,0)" OR name = "(6,11)" THEN "send_groupchat_message"
        WHEN name = "(5,37)"  THEN "open_subscribe_message_list"
    END AS request_url,
    CAST(total_count AS BIGINT) AS total_num,
    CAST(success_count AS BIGINT)  AS succeed_num,
    CAST((avg_time*total_count) AS FLOAT)  AS request_time
    FROM
    message_kpi
    ) temp
    WHERE request_url IS NOT NULL
    GROUP BY request_url










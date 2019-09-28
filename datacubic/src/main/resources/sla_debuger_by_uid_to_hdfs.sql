SELECT
	uid,
	system,
	sum(net_time) as net_time,
	sum(total_num) as total_num,
	sum(net_time) / 1.0 / sum(total_num) as avg_net_time
FROM
	(
		SELECT
		    uid,
		    uainfos["app_version"] as app_version,
		    uainfos["system"] as system,
		    request_url,
		    net_time,
		    total_num
		FROM
			(
				SELECT
				    uid,
					parseUAInfo(ua) as uainfos,
					CASE
                        WHEN request_url IS NOT NULL THEN split(request_url, "\\?")[0]
                        ELSE "NULL"
                    END as request_url,
				    CAST(net_time AS BIGINT) AS net_time,
				    1 AS total_num
				FROM
					source_table
				WHERE
				    subtype = "refresh_feed"
					AND net_time IS NOT NULL
					AND uid IS NOT NULL
					AND substring(uid, length(uid)-2, 2) = "01"
			) temp
	) temp2
WHERE
    app_version = "7.7.1"
    AND request_url LIKE "%unread_friends_timeline"
GROUP BY
	uid,
	system
SELECT
    _timestamp,
    subtype,
    uainfos["app_version"] as app_version,
    uainfos["system"] as system,
    uainfos["system_version"] as system_version,
    network_type,
    iplocations["province"] as province,
    iplocations["isp"] as isp,
    sch,
    request_url,
    sessionid,
    uid,
    during_time,
    net_time,
    parse_time,
    local_time,
    start_time,
    total_num
FROM
	(
		SELECT
		    time_to_utc_with_interval(@TIMESTAMP, "day") AS _timestamp,
			subtype,
			parseUAInfo(ua) as uainfos,
			network_type,
			ipToLocation(ip) as iplocations,
			sch,
			CASE
            	WHEN request_url IS NOT NULL THEN split(request_url, "\\?")[0]
                ELSE NULL
            END as request_url,
            sessionid,
            uid,
            CASE
            	WHEN during_time IS NOT NULL THEN CAST(during_time AS DOUBLE)
            	ELSE -1.0
           	END as during_time,
            CASE
            	WHEN net_time IS NOT NULL THEN CAST(net_time AS DOUBLE)
            	ELSE -1.0
           	END as net_time,
		    CASE
                WHEN parseTime IS NOT NULL THEN CAST(parseTime AS DOUBLE)
                ELSE 0
            END as parse_time,
		    CAST((during_time - net_time) AS DOUBLE) AS local_time,
		    CAST(start_time AS DOUBLE) AS start_time,
		    1 AS total_num
		FROM
			source_table
		WHERE
		    subtype = "refresh_feed"
	) temp
SELECT
	_timestamp,
	subtype,
	app_version,
	system,
	system_version,
	network_type,
	province,
	isp,
	sch,
	request_url,
	firstframe_status,
    quit_status,
    trace_dns_province,
    cache_type,
    video_type,
	sum(during_time) as during_time,
	sum(net_time) as net_time,
	sum(parse_time) as parse_time,
	sum(local_time) as local_time,
	sum(total_num) as total_num,
	sum(during_time_1s) as during_time_1s,
	sum(during_time_2s) as during_time_2s,
	sum(during_time_3s) as during_time_3s,
	sum(during_time_4s) as during_time_4s,
	sum(during_time_5s) as during_time_5s,
	sum(during_time_long) as during_time_long,
	sum(net_time_1s) as net_time_1s,
	sum(net_time_2s) as net_time_2s,
	sum(net_time_3s) as net_time_3s,
	sum(net_time_4s) as net_time_4s,
	sum(net_time_5s) as net_time_5s,
	sum(net_time_long) as net_time_long,
	sum(parse_time_1s) as parse_time_1s,
	sum(parse_time_2s) as parse_time_2s,
	sum(parse_time_3s) as parse_time_3s,
	sum(parse_time_4s) as parse_time_4s,
	sum(parse_time_5s) as parse_time_5s,
	sum(parse_time_long) as parse_time_long
FROM
	(
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
		    firstframe_status,
            quit_status,
            CASE
                WHEN trace_dns_iplocations IS NOT NULL THEN trace_dns_iplocations["province"]
                ELSE "NULL"
            END AS trace_dns_province,
            cache_type,
            video_type,
		    during_time,
		    net_time,
		    parse_time,
		    local_time,
		    total_num,
			CASE
		 	    WHEN during_time <= 1000 THEN 1
		 	    ELSE 0
		 	END AS during_time_1s,
		 	CASE
		 	    WHEN during_time > 1000 AND during_time <= 2000 THEN 1
		 	    ELSE 0
		 	END AS during_time_2s,
		 	CASE
		 	    WHEN during_time > 2000 AND during_time <= 3000 THEN 1
		 	    ELSE 0
		 	END AS during_time_3s,
		 	CASE
		 	    WHEN during_time > 3000 AND during_time <= 4000 THEN 1
		 	    ELSE 0
		 	END AS during_time_4s,
		 	CASE
		 	    WHEN during_time > 4000 AND during_time <= 5000 THEN 1
		 	    ELSE 0
		 	END AS during_time_5s,
		 	CASE
		 	    WHEN during_time > 5000 THEN 1
		 	    ELSE 0
		 	END AS during_time_long,
			CASE
			    WHEN net_time <= 1000 THEN 1
			    ELSE 0
			END AS net_time_1s,
			CASE
			    WHEN net_time > 1000 AND net_time <= 2000 THEN 1
			    ELSE 0
			END AS net_time_2s,
			CASE
			    WHEN net_time > 2000 AND net_time <= 3000 THEN 1
			    ELSE 0
			END AS net_time_3s,
			CASE
			    WHEN net_time > 3000 AND net_time <= 4000 THEN 1
			    ELSE 0
			END AS net_time_4s,
			CASE
			    WHEN net_time > 4000 AND net_time <= 5000 THEN 1
			    ELSE 0
			END AS net_time_5s,
			CASE
			    WHEN net_time > 5000 THEN 1
			    ELSE 0
			END AS net_time_long,
			CASE
			    WHEN parse_time <= 1000 THEN 1
			    ELSE 0
			END AS parse_time_1s,
			CASE
			    WHEN parse_time > 1000 AND parse_time <= 2000 THEN 1
			    ELSE 0
			END AS parse_time_2s,
			CASE
			    WHEN parse_time > 2000 AND parse_time <= 3000 THEN 1
			    ELSE 0
			END AS parse_time_3s,
			CASE
			    WHEN parse_time > 3000 AND parse_time <= 4000 THEN 1
			    ELSE 0
			END AS parse_time_4s,
			CASE
			    WHEN parse_time > 4000 AND parse_time <= 5000 THEN 1
			    ELSE 0
			END AS parse_time_5s,
			CASE
			    WHEN parse_time > 5000 THEN 1
			    ELSE 0
			END AS parse_time_long
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
                        WHEN subtype = "refresh_feed" AND request_url IS NOT NULL THEN split(request_url, "\\?")[0]
                        ELSE "NULL"
                    END as request_url,
					firstframe_status,
                    quit_status,
                    CASE
                        WHEN trace_dns_ip IS NOT NULL THEN ipToLocation(trace_dns_ip)
                    	ELSE NULL
                    END AS trace_dns_iplocations,
					CASE
					    WHEN cache_type IS NOT NULL THEN cache_type
					    ELSE "NULL"
					END AS cache_type,
					CASE
					    WHEN objectid IS NOT NULL THEN split(objectid, ":")[0]
					    ELSE "NULL"
					END AS video_type,
                    CAST(during_time AS BIGINT) AS during_time,
				    CAST(net_time AS BIGINT) AS net_time,
				    CASE
                        WHEN parseTime IS NOT NULL THEN CAST(parseTime AS BIGINT)
                        ELSE 0
                    END as parse_time,
				    CAST((during_time - net_time) AS BIGINT) AS local_time,
				    1 AS total_num
				FROM
					source_table
				WHERE
				    (
				        (subtype = "refresh_feed" AND parseTime IS NOT NULL)
                        OR
                        subtype = "play_video"
				    )
					AND isnumber(result_code) = TRUE
					AND CAST(CAST(CAST(result_code AS DOUBLE) AS BIGINT) AS STRING) = "0"
					AND during_time IS NOT NULL
					AND net_time IS NOT NULL
			) temp
	) temp2
WHERE
    during_time >= 0 AND during_time < 60000
    AND net_time >= 0 AND net_time < 60000
    AND parse_time >= 0 AND parse_time < 60000
    AND local_time >= 0 AND local_time < 60000
GROUP BY
	_timestamp,
	subtype,
	app_version,
	system,
	system_version,
	network_type,
	province,
	isp,
	sch,
	request_url,
	firstframe_status,
    quit_status,
    trace_dns_province,
    cache_type,
    video_type
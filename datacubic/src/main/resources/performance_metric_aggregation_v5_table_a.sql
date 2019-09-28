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
	result_code,
	ne,
	firstframe_status,
    quit_status,
    trace_dns_province,
    cache_type,
    video_type,
   	sum(total_num) as total_num,
    sum(succeed_num) as succeed_num,
    sum(succeed_num_during_time_1_5s) as succeed_num_during_time_1_5s,
    sum(succeed_num_net_time_1_5s) as succeed_num_net_time_1_5s,
    sum(business_error_num) as business_error_num
FROM
	(
		SELECT
			time_to_utc_with_interval(@TIMESTAMP, "day") AS _timestamp,
		    subtype,
		    uainfos["app_version"] AS app_version,
		    uainfos["system"] AS system,
		    uainfos["system_version"] AS system_version,
		    network_type,
		    iplocations["province"] AS province,
		    iplocations["isp"] AS isp,
		    sch,
		    request_url,
		    result_code,
		    ne,
		    firstframe_status,
            quit_status,
            CASE
                WHEN trace_dns_iplocations IS NOT NULL THEN trace_dns_iplocations["province"]
                ELSE "NULL"
            END AS trace_dns_province,
            cache_type,
            video_type,
		    total_num,
			CASE
				WHEN result_code IN ("0", "-8108", "-4501", "-4098", "-4010", "-1019", "-1018", "-1011", "-1010", "-1008", "-1007", "-1005", "-9", "-105", "-100", "-200", "1", "5", "1014", "9109", "20003", "20012", "20015", "20016", "20017", "20018", "20019", "20020", "20021", "20031", "20034", "20046", "20101", "20201", "20130", "20134", "20135", "20148", "20156", "20206", "20208", "20210", "20603", "107002", "50112071", "1001030042", "1078030002") THEN 1
			    ELSE 0
			END AS succeed_num,
			CASE
				WHEN result_code IN ("0", "-8108", "-4501", "-4098", "-4010", "-1019", "-1018", "-1011", "-1010", "-1008", "-1007", "-1005", "-9", "-105", "-100", "-200", "1", "5", "1014", "9109", "20003", "20012", "20015", "20016", "20017", "20018", "20019", "20020", "20021", "20031", "20034", "20046", "20101", "20201", "20130", "20134", "20135", "20148", "20156", "20206", "20208", "20210", "20603", "107002", "50112071", "1001030042", "1078030002") AND during_time <= 1500 THEN 1
			    ELSE 0
			END AS succeed_num_during_time_1_5s,
			CASE
				WHEN result_code IN ("0", "-8108", "-4501", "-4098", "-4010", "-1019", "-1018", "-1011", "-1010", "-1008", "-1007", "-1005", "-9", "-105", "-100", "-200", "1", "5", "1014", "9109", "20003", "20012", "20015", "20016", "20017", "20018", "20019", "20020", "20021", "20031", "20034", "20046", "20101", "20201", "20130", "20134", "20135", "20148", "20156", "20206", "20208", "20210", "20603", "107002", "50112071", "1001030042", "1078030002") AND net_time <= 1500 THEN 1
			    ELSE 0
			END AS succeed_num_net_time_1_5s,
			CASE
		        WHEN result_code IN ("-8108", "-4501", "-4098", "-4010", "-1019", "-1018", "-1011", "-1010", "-1008", "-1007", "-1005", "-9", "-105", "-100", "-200", "1", "5", "1014", "9109", "20003", "20012", "20015", "20016", "20017", "20018", "20019", "20020", "20021", "20031", "20034", "20046", "20101", "20201", "20130", "20134", "20135", "20148", "20156", "20206", "20208", "20210", "20603", "107002", "50112071", "1001030042", "1078030002") THEN 1
			    ELSE 0
			END AS business_error_num
		FROM
			(
				SELECT
					subtype,
					parseUAInfo(ua) AS uainfos,
					network_type,
					ipToLocation(ip) AS iplocations,
					sch,
					CASE
                        WHEN subtype = "refresh_feed" AND request_url IS NOT NULL THEN split(request_url, "\\?")[0]
                    	ELSE "NULL"
                    END AS request_url,
					CASE
                        WHEN isnumber(result_code) THEN cast(cast(cast(result_code as double) as bigint) as string)
                    	ELSE "NULL"
                    END AS result_code,
					ne,
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
					1 AS total_num
				FROM
					source_table
				WHERE
					subtype IN ("refresh_feed", "play_video")
			) temp
	) temp2
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
	result_code,
	ne,
	firstframe_status,
    quit_status,
    trace_dns_province,
    cache_type,
    video_type
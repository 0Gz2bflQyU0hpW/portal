SELECT
	*
FROM
	(
		SELECT
		    subtype,
		    CASE
		        WHEN time IS NOT NULL THEN time_to_utc_with_interval(CAST(time as BIGINT), 'day')
		        ELSE NULL
		    END AS _timestamp,
		    ua,
		    parseUAInfo(ua)['app_version'] AS app_version,
		    parseUAInfo(ua)['system'] AS system,
		    parseUAInfo(ua)['system_version'] AS system_version,
		    network_type,
		    ipToLocation(ip)['province'] AS province,
		    ipToLocation(ip)['isp'] AS isp,
		    sch,
		    request_url,
		    1 AS total_num,
		    CASE
		        WHEN result_code IN ('0', '-8108', '-4501', '-4098', '-4010', '-1019', '-1018', '-1011', '-1010', '-1008', '-1007', '-1005', '-9', '-105', '-100', '1', '5', '1014', '9109', '20003', '20012', '20015', '20016', '20017', '20018', '20019', '20020', '20021', '20031', '20034', '20046', '20101', '20201', '20130', '20134', '20135', '20148', '20156', '20206', '20208', '20210', '20603', '107002', '50112071', '1001030042', '1078030002') THEN 1
		        ELSE 0
		    END as succeed_num,
		    CASE
		        WHEN result_code IN ('-1202', '-1020', '-1017', '-1009', '-1006', '-1004', '-1003', '-1001', '-200', '-4', '-1', '2', '6', '22', '54', '205', '302', '303', '307', '400', '403', '404', '405', '500', '502', '503', '504', '705', '706', '1101', '7002', '7003', '7004', '105001', '3020003') THEN 1
		        ELSE 0
		    END AS neterr_num,
		    CASE
		        WHEN result_code IN ('7', '7001', '3840', '7005', '8998', '9102', '20120', '20205', '201603', 'e000', '3', '4', '1040002', '3020005', '3020006', '10001', '10009', '10011') THEN 1
		        ELSE 0
		    END AS localerr_num,
		    CASE
		        WHEN during_time IS NOT NULL THEN CAST(during_time AS BIGINT)
		        ELSE 0
		    END AS during_time,
		    CASE
		        WHEN net_time IS NOT NULL THEN CAST(net_time AS BIGINT)
		        ELSE 0
		    END AS net_time,
		    CAST((during_time - net_time) AS BIGINT) AS local_time,
		    CASE
		        WHEN parseTime IS NOT NULL THEN CAST(parseTime AS BIGINT)
		        ELSE 0
		    END AS parse_time,
		    CASE
		        WHEN lw IS NOT NULL THEN CAST(lw AS BIGINT)
		        ELSE 0
		    END AS lw,
		    CASE
		        WHEN dl IS NOT NULL THEN CAST(dl AS BIGINT)
		        ELSE 0
		    END AS dl,
		    CASE
		        WHEN sc IS NOT NULL THEN CAST(sc AS BIGINT)
		        ELSE 0
		    END AS sc,
		    CASE
		        WHEN ssc IS NOT NULL THEN CAST(ssc AS BIGINT)
		        ELSE 0
		    END AS ssc,
		    CASE
		        WHEN sr IS NOT NULL THEN CAST(sr AS BIGINT)
		        ELSE 0
		    END AS sr,
		    CASE
		        WHEN ws IS NOT NULL THEN CAST(ws AS BIGINT)
		        ELSE 0
		    END AS ws,
		    CASE
		        WHEN rh IS NOT NULL THEN CAST(rh AS BIGINT)
		        ELSE 0
		    END AS rh,
		    CASE
		        WHEN rb IS NOT NULL THEN CAST(rb AS BIGINT)
		        ELSE 0
		    END AS rb,
		    CASE
		        WHEN ne = 'cn0' THEN 0
		        ELSE 1
		    END AS ne
		FROM
		    source_table
	) translate_table
WHERE
	system IS NULL
LIMIT 10
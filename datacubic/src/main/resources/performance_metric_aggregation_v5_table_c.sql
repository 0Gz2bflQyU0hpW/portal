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
	sum(dl) as dl,
	sum(sc) as sc,
	sum(ssc) as ssc,
	sum(sr) as sr,
	sum(ws) as ws,
	sum(rb) as rb,
	sum(start_cronet) as start_cronet,
	sum(cronet_dl) as cronet_dl,
	sum(ws_rb) as ws_rb,
	sum(cronet_finish) as cronet_finish,
	sum(ahead_net_localtime) as ahead_net_localtime,
	sum(behind_net_localtime) as behind_net_localtime,
	sum(total_num) as total_num
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
			dl,
			sc,
			ssc,
			sr,
			ws,
			rb,
			start_cronet,
			cronet_dl,
			ws_rb,
			cronet_finish,
			ahead_net_localtime,
			behind_net_localtime,
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
                    	ELSE "NULL"
                    END AS request_url,
				    CAST(dl AS BIGINT) AS dl,
				    CAST(sc AS BIGINT) AS sc,
				    CAST(ssc AS BIGINT) AS ssc,
				    CAST(sr AS BIGINT) AS sr,
				    CAST(ws AS BIGINT) AS ws,
				    CAST(rb AS BIGINT) AS rb,
				    CAST(start_cronet AS BIGINT) AS start_cronet,
				    CAST(cronet_dl AS BIGINT) AS cronet_dl,
				    CAST(ws_rb AS BIGINT) AS ws_rb,
				    CAST(cronet_finish AS BIGINT) AS cronet_finish,
				    CAST(ahead_net_localtime AS BIGINT) AS ahead_net_localtime,
				    CAST(behind_net_localtime AS BIGINT) AS behind_net_localtime,
				    1 AS total_num
				FROM
					source_table
				WHERE
					subtype = "refresh_feed"
					AND isnumber(result_code) = TRUE
                    AND CAST(CAST(CAST(result_code AS DOUBLE) AS BIGINT) AS STRING) = "0"
					AND dl IS NOT NULL
					AND sc IS NOT NULL
					AND ssc IS NOT NULL
					AND sr IS NOT NULL
					AND ws IS NOT NULL
					AND rb IS NOT NULL
					AND start_cronet IS NOT NULL
					AND cronet_dl IS NOT NULL
					AND ws_rb IS NOT NULL
					AND cronet_finish IS NOT NULL
					AND ahead_net_localtime IS NOT NULL
					AND behind_net_localtime IS NOT NULL
			) temp
	) temp2
WHERE
    dl >= 0 AND dl < 10000
    AND sc >= 0 AND sc < 10000
	AND ssc >= 0 AND ssc < 10000
	AND sr >= 0 AND sr < 10000
	AND ws >= 0 AND ws < 10000
	AND rb >= 0 AND rb < 10000
	AND start_cronet >= 0 AND start_cronet < 10000
	AND cronet_dl >= 0 AND cronet_dl < 10000
	AND ws_rb >= 0 AND ws_rb < 10000
	AND cronet_finish >= 0 AND cronet_finish < 10000
	AND ahead_net_localtime >= 0 AND ahead_net_localtime < 10000
	AND behind_net_localtime >= 0 AND behind_net_localtime < 10000
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
	request_url
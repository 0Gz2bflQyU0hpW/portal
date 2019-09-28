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
	sum(lw) as lw,
	sum(dl) as dl,
	sum(sc) as sc,
	sum(ssc) as ssc,
	sum(sr) as sr,
	sum(ws) as ws,
	sum(rh) as rh,
	sum(rb) as rb,
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
			lw,
			dl,
			sc,
			ssc,
			sr,
			ws,
			rh,
			rb,
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
				    CAST(lw AS BIGINT) AS lw,
				    CAST(dl AS BIGINT) AS dl,
				    CAST(sc AS BIGINT) AS sc,
				    CAST(ssc AS BIGINT) AS ssc,
				    CAST(sr AS BIGINT) AS sr,
				    CAST(ws AS BIGINT) AS ws,
				    CAST(rh AS BIGINT) AS rh,
				    CAST(rb AS BIGINT) AS rb,
				    1 AS total_num
				FROM
					source_table
				WHERE
					subtype = "refresh_feed"
					AND isnumber(result_code) = TRUE
                    AND CAST(CAST(CAST(result_code AS DOUBLE) AS BIGINT) AS STRING) = "0"
					AND lw IS NOT NULL
					AND dl IS NOT NULL
					AND sc IS NOT NULL
					AND ssc IS NOT NULL
					AND sr IS NOT NULL
					AND ws IS NOT NULL
					AND rh IS NOT NULL
					AND rb IS NOT NULL
			) temp
	) temp2
WHERE
    lw >= 0 AND lw < 10000
    AND dl >= 0 AND dl < 10000
    AND sc >= 0 AND sc < 10000
	AND ssc >= 0 AND ssc < 10000
	AND sr >= 0 AND sr < 10000
	AND ws >= 0 AND ws < 10000
	AND rh >= 0 AND rh < 10000
	AND rb >= 0 AND rb < 10000
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
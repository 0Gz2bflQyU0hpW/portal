SELECT
    source_line
FROM
	(
		SELECT
		    source_line,
		    uainfos["system"] as system
		FROM
			(
				SELECT
				    source_line,
					parseUAInfo(ua) as uainfos
				FROM
					source_table
				WHERE
					subtype = "refresh_feed"
			) temp
	) temp2
WHERE
    system IS NULL
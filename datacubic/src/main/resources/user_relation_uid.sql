SELECT *
FROM (SELECT oref, uid, COUNT(uid) AS uidcount
	FROM (SELECT *
		FROM openapi_op
		WHERE oref IN (?)
		) temp
	GROUP BY oref, uid
	) tmp
WHERE uidcount >= 10
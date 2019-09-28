SELECT *
FROM (SELECT oref, COUNT(1) AS orefcount
	FROM openapi_op
	WHERE uaction = '14000008'
		OR uaction = '14000009'
	GROUP BY oref
	) temp
WHERE orefcount > 1000000
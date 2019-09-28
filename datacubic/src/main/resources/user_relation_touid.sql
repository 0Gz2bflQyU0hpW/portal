SELECT oref, uid, touid, 1 AS touidcount
FROM (
      SELECT oref, uid, touid, filterOrefUid(oref, uid) AS istrue
	    FROM openapi_op
	   ) tmp1
WHERE istrue = 'true'
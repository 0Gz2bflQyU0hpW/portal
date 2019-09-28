SELECT sum(platform_during) AS platform_during,
       sum(wesync_during) AS wesync_during,
       sum(total_num) AS total_num,
       sum(during_time) AS during_time,
       sum(during_time_250ms) AS during_time_250ms,
       sum(during_time_500ms) AS during_time_500ms,
       sum(during_time_750ms) AS during_time_750ms,
       sum(during_time_1000ms) AS during_time_1000ms,
       sum(during_time_1500ms) AS during_time_1500ms,
       sum(during_time_2000ms) AS during_time_2000ms,
       sum(during_time_long) AS during_time_long
FROM (SELECT platform_during,
       wesync_during,
       total_num,
       during_time,
       CASE
		 	     WHEN during_time > 0 AND during_time <= 250 THEN 1
		 	     ELSE 0
		 	  END AS during_time_250ms,
		   CASE
		 	     WHEN during_time > 250 AND during_time <= 500 THEN 1
		 	     ELSE 0
		 	 END AS during_time_500ms,
		 	 CASE
		 	     WHEN during_time > 500 AND during_time <= 750 THEN 1
		 	     ELSE 0
		 	  END AS during_time_750ms,
		 	  CASE
		 	     WHEN during_time > 750 AND during_time <= 1000 THEN 1
		 	     ELSE 0
		 	  END AS during_time_1000ms,
		 	  CASE
		 	     WHEN during_time > 1000 AND during_time <= 1500 THEN 1
		 	     ELSE 0
		 	  END AS during_time_1500ms,
		 	   CASE
		 	     WHEN during_time > 1500 AND during_time <= 2000 THEN 1
		 	     ELSE 0
		 	  END AS during_time_2000ms,
		 	  CASE
		 	     WHEN during_time > 2000  THEN 1
		 	     ELSE 0
		 	  END AS during_time_long
FROM(	SELECT platform_during,
       wesync_during,
       total_num,
       CAST((platform_during + wesync_during) AS FLOAT ) AS during_time
FROM (SELECT
    CASE WHEN platformCost >=0 THEN CAST(platformCost AS FLOAT )
         ELSE 0.00
    END AS platform_during,
    CASE WHEN wesyncCost >= 0 THEN CAST(wesyncCost AS FLOAT )
         ELSE 0.00
    END AS wesync_during,
    1 AS total_num
    FROM
    message_kpi
    WHERE type = "online"
    ) temp
  ) temp1
 ) temp2

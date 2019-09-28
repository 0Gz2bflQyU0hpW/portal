SELECT sum(platform_during) AS platform_during,
       sum(notice_center_during) AS notice_center_during,
       sum(mps_during) AS mps_during,
       sum(total_num) AS total_num,
       sum(during_time) AS during_time,
       sum(during_time_200ms) AS during_time_200ms,
       sum(during_time_400ms) AS during_time_400ms,
       sum(during_time_600ms) AS during_time_600ms,
       sum(during_time_800ms) AS during_time_800ms,
       sum(during_time_1000ms) AS during_time_1000ms,
       sum(during_time_1500ms) AS during_time_1500ms,
       sum(during_time_long) AS during_time_long
FROM (SELECT platform_during,
       notice_center_during,
       mps_during,
       total_num,
       during_time,
       CASE
		 	     WHEN during_time > 0 AND during_time <= 200 THEN 1
		 	     ELSE 0
		 	  END AS during_time_200ms,
		   CASE
		 	     WHEN during_time > 200 AND during_time <= 400 THEN 1
		 	     ELSE 0
		 	  END AS during_time_400ms,
		 	 CASE
		 	     WHEN during_time > 400 AND during_time <= 600 THEN 1
		 	     ELSE 0
		 	  END AS during_time_600ms,
		 	  CASE
		 	     WHEN during_time > 600 AND during_time <= 800 THEN 1
		 	     ELSE 0
		 	  END AS during_time_800ms,
		 	  CASE
		 	     WHEN during_time > 800 AND during_time <= 1000 THEN 1
		 	     ELSE 0
		 	  END AS during_time_1000ms,
		 	   CASE
		 	     WHEN during_time > 1000 AND during_time <= 1500 THEN 1
		 	     ELSE 0
		 	  END AS during_time_1500ms,
		 	  CASE
		 	     WHEN during_time > 1500  THEN 1
		 	     ELSE 0
		 	  END AS during_time_long
FROM(	SELECT platform_during,
       notice_center_during,
       mps_during,
       total_num,
       CAST((platform_during + notice_center_during + mps_during) AS FLOAT ) AS during_time
FROM (SELECT
    CASE WHEN platformCost >=0 THEN CAST(platformCost AS FLOAT )
         ELSE 0.00
    END AS platform_during,
    CASE WHEN noticeCenterCost >= 0 THEN CAST(noticeCenterCost AS FLOAT )
         ELSE 0.00
    END AS notice_center_during,
    CASE WHEN mpsCost >=0 THEN CAST(mpsCost AS FLOAT)
         ELSE 0.00
    END AS mps_during,
    1 AS total_num
    FROM
    message_kpi
    WHERE type = "offline"
    ) temp
  ) temp1
) temp2
